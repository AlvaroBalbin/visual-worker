// index.js — Visual worker for Social Gravity / FocusGroup
// Pure JavaScript (no TypeScript annotations)

import { createClient } from '@supabase/supabase-js';
import { exec } from 'child_process';
import fs from 'fs';
import fetch from 'node-fetch';
import OpenAI from 'openai';

// --- Env + clients ----------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const openaiKey = process.env.OPENAI_API_KEY;

// Tuning knobs for frames
const DEFAULT_FPS = parseFloat(process.env.VISUAL_FRAMES_FPS || '2'); // was 1
const MAX_FRAMES = parseInt(process.env.VISUAL_MAX_FRAMES || '120', 10); // cap uploads

if (!supabaseUrl || !supabaseServiceKey || !openaiKey) {
  console.error(
    'Missing env vars SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / OPENAI_API_KEY'
  );
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);
const openai = new OpenAI({ apiKey: openaiKey });

// --- Small helpers ----------------------------------------------------------

function execPromise(cmd) {
  return new Promise((resolve, reject) => {
    exec(cmd, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- Core worker logic ------------------------------------------------------

async function processVisualJob() {
  console.log('Checking for pending/failed visual_jobs…');

  // Pick up BOTH "pending" and "failed" so retries work
  const { data: jobs, error } = await supabase
    .from('visual_jobs')
    .select('id, simulation_id, status')
    .in('status', ['pending', 'failed'])
    .limit(1);

  if (error) {
    console.error('visual_jobs select error:', error);
    return;
  }

  if (!jobs || jobs.length === 0) {
    console.log('No pending/failed jobs.');
    return;
  }

  const job = jobs[0];
  console.log(
    `Processing job ${job.id} (status=${job.status}) for simulation ${job.simulation_id}`
  );

  // Mark as processing
  await supabase
    .from('visual_jobs')
    .update({ status: 'processing', error_message: null })
    .eq('id', job.id);

  try {
    // Load simulation (we need video_url, transcript and ideally duration)
    const { data: sim, error: simErr } = await supabase
      .from('simulations')
      .select('video_url, transcript, video_duration_seconds')
      .eq('id', job.simulation_id)
      .single();

    if (simErr) throw simErr;
    if (!sim || !sim.video_url) throw new Error('No video_url on simulation');

    const videoDurationSeconds = sim.video_duration_seconds || 0;

    console.log('Downloading video:', sim.video_url);

    const videoRes = await fetch(sim.video_url);
    if (!videoRes.ok) {
      throw new Error(
        `Failed to fetch video: ${videoRes.status} ${videoRes.statusText}`
      );
    }

    const arrayBuf = await videoRes.arrayBuffer();
    const videoPath = `/tmp/${job.id}.mp4`;
    fs.writeFileSync(videoPath, Buffer.from(arrayBuf));

    const framesDir = `/tmp/frames-${job.id}`;
    fs.mkdirSync(framesDir, { recursive: true });

    console.log(
      `Extracting frames with ffmpeg at ~${DEFAULT_FPS} fps for higher visual coverage…`
    );

    // Higher FPS for better coverage; you can adjust via VISUAL_FRAMES_FPS env var
    await execPromise(
      `ffmpeg -y -i ${videoPath} -vf "fps=${DEFAULT_FPS}" ${framesDir}/frame-%05d.jpg`
    );

    let frameFiles = fs
      .readdirSync(framesDir)
      .filter((f) => f.toLowerCase().endsWith('.jpg'))
      .sort(); // ensure chronological order

    console.log('Extracted', frameFiles.length, 'raw frames');

    // Downsample if we have too many frames (keep evenly-spaced subset)
    if (frameFiles.length > MAX_FRAMES) {
      const step = Math.ceil(frameFiles.length / MAX_FRAMES);
      frameFiles = frameFiles.filter((_, idx) => idx % step === 0);
      console.log(
        `Downsampled frames to ${frameFiles.length} (MAX_FRAMES=${MAX_FRAMES}, step=${step})`
      );
    }

    const frameUrls = [];
    const frameTimeline = [];

    console.log('Uploading', frameFiles.length, 'frames to storage…');

    const frameCount = frameFiles.length;

    for (let i = 0; i < frameFiles.length; i++) {
      const file = frameFiles[i];
      const fullPath = `${framesDir}/${file}`;
      const buf = fs.readFileSync(fullPath);
      const uploadPath = `${job.simulation_id}/${job.id}/${file}`;

      const { error: uploadErr } = await supabase.storage
        .from('video-frames')
        .upload(uploadPath, buf, {
          contentType: 'image/jpeg',
          upsert: true,
        });

      if (uploadErr) {
        console.error('Frame upload error for', uploadPath, uploadErr);
        throw uploadErr;
      }

      const { data: publicData } = supabase.storage
        .from('video-frames')
        .getPublicUrl(uploadPath);

      if (publicData?.publicUrl) {
        const url = publicData.publicUrl;
        frameUrls.push(url);

        // Compute an approximate timestamp for this frame.
        // If we know the video duration, spread frames across it.
        // Otherwise, fall back to FPS-based timing.
        let tsSeconds;
        if (videoDurationSeconds && videoDurationSeconds > 0 && frameCount > 0) {
          const position = (i + 1) / (frameCount + 1); // 0..1
          tsSeconds = position * videoDurationSeconds;
        } else {
          tsSeconds = (i + 0.5) / DEFAULT_FPS;
        }

        frameTimeline.push({
          index: i,
          url,
          timestamp_seconds: Number(tsSeconds.toFixed(2)),
        });
      }
    }

    // --- OpenAI visual analysis (coarse + visual events) ---------------------

    const framesListing = frameTimeline
      .map(
        (f) =>
          `[${f.index}] t=${f.timestamp_seconds.toFixed(
            2
          )}s → ${f.url}`
      )
      .join('\n');

    const visualPrompt = [
      'You are a short-form video creative strategist.',
      'You will receive:',
      '- A sequence of frame image URLs extracted evenly from a short-form video.',
      '- Each frame has an index and an approximate timestamp in seconds from the start.',
      '- The full transcript of the video (optional).',
      '',
      'Your tasks:',
      '1) Describe what the video looks like and how it feels to watch.',
      '2) Group the frames into 3–8 key visual beats / events (scenes, important moments).',
      '3) For each event, reference frame indices ONLY (the backend will map indices to exact times).',
      '',
      'Return ONLY valid JSON with this exact schema:',
      '',
      '{',
      '  "visual_style": "string (1–2 sentences about the overall look & vibe)",',
      '  "scene_summary": "string (2–3 sentences summarizing what visually happens in the video)",',
      '  "aesthetic_tags": ["string", "..."],',
      '  "pacing_description": "string (how fast the video feels and how the cuts/movements behave)",',
      '  "quality_assessment": {',
      '    "resolution_ok": true,',
      '    "lighting_ok": "good | okay | poor",',
      '    "edit_quality": "simple | basic | advanced | chaotic"',
      '  },',
      '  "on_screen_text_usage": "string describing lower-thirds, captions, UI text, etc.",',
      '',
      '  "visual_events": [',
      '    {',
      '      "label": "short description of a visual beat or scene",',
      '      "start_frame_index": 0,            // integer index into the frames list below',
      '      "end_frame_index": 5,             // inclusive, integer index (>= start_frame_index)',
      '      "notes_for_editor": [             // 1–3 bullets with visual/editing notes',
      '        "string",',
      '        "string"',
      '      ]',
      '    }',
      '  ]',
      '}',
      '',
      'Rules for visual_events:',
      '- Use ONLY integer frame indices that exist in the frames list.',
      '- start_frame_index must be >= 0 and < number_of_frames.',
      '- end_frame_index must be >= start_frame_index and < number_of_frames.',
      '- Prefer 3–8 events total.',
      '',
      'Frames (index, timestamp, URL):',
      framesListing,
      '',
      'Transcript (optional, for context):',
      sim.transcript || '(no transcript available)',
    ].join('\n');

    console.log('Calling OpenAI for base visual analysis + visual events…');

    const completion = await openai.chat.completions.create({
      model: 'gpt-5.1',
      response_format: { type: 'json_object' },
      messages: [
        {
          role: 'system',
          content:
            'You are a visual strategist for TikTok/Reels shorts. Focus on visuals and perceived pacing, group frames into key visual events, and always return strict JSON.',
        },
        { role: 'user', content: visualPrompt },
      ],
    });

    const content = completion.choices?.[0]?.message?.content;
    if (!content) throw new Error('Empty visual analysis response');

    const visualAnalysis = JSON.parse(content);

    // --- Post-process visual_events: convert frame indices → seconds ---------

    const rawEvents = Array.isArray(visualAnalysis.visual_events)
      ? visualAnalysis.visual_events
      : [];

    const eventsWithTimes = rawEvents
      .map((ev) => {
        if (!ev) return null;

        const label =
          typeof ev.label === 'string' ? ev.label.trim() : '';

        if (!label) return null;

        const startIdxRaw =
          typeof ev.start_frame_index === 'number'
            ? ev.start_frame_index
            : 0;
        const endIdxRaw =
          typeof ev.end_frame_index === 'number'
            ? ev.end_frame_index
            : startIdxRaw;

        const maxIndex = frameTimeline.length - 1;
        const startIdx = Math.max(
          0,
          Math.min(startIdxRaw | 0, maxIndex)
        );
        const endIdx = Math.max(
          startIdx,
          Math.min(endIdxRaw | 0, maxIndex)
        );

        const startFrame = frameTimeline[startIdx];
        const endFrame = frameTimeline[endIdx];

        const startSeconds = startFrame
          ? startFrame.timestamp_seconds
          : 0;
        const endSeconds = endFrame
          ? endFrame.timestamp_seconds
          : startSeconds;

        const notes =
          Array.isArray(ev.notes_for_editor) &&
          ev.notes_for_editor.length
            ? ev.notes_for_editor.map((n) =>
                typeof n === 'string' ? n.trim() : ''
              ).filter(Boolean)
            : [];

        return {
          label,
          start_frame_index: startIdx,
          end_frame_index: endIdx,
          start_seconds: Number(startSeconds.toFixed(2)),
          end_seconds: Number(endSeconds.toFixed(2)),
          notes_for_editor: notes,
        };
      })
      .filter(Boolean);

    visualAnalysis.frame_timeline = frameTimeline;
    visualAnalysis.visual_events = eventsWithTimes;

    // --- Save back to Supabase ---------------------------------------------

    console.log('Saving visual analysis to Supabase…');

    await supabase
      .from('visual_jobs')
      .update({
        status: 'complete',
        frames: frameUrls,
        visual_analysis: visualAnalysis,
        error_message: null,
      })
      .eq('id', job.id);

    // Store initial visual_analysis on the simulation;
    // analyze_simulation will later enrich this with storytelling/editing insights.
    await supabase
      .from('simulations')
      .update({ visual_analysis: visualAnalysis })
      .eq('id', job.simulation_id);

    console.log('Job completed:', job.id);
  } catch (err) {
    console.error('processVisualJob error:', err);

    await supabase
      .from('visual_jobs')
      .update({
        status: 'failed',
        error_message: String(err),
      })
      .eq('id', job.id);
  }
}

// --- Main loop --------------------------------------------------------------

async function mainLoop() {
  console.log('Visual worker running…');
  while (true) {
    try {
      await processVisualJob();
    } catch (err) {
      console.error('Worker error (outer):', err);
    }
    await sleep(3000);
  }
}

mainLoop();
