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
    // Load simulation (we need video_url and transcript)
    const { data: sim, error: simErr } = await supabase
      .from('simulations')
      .select('video_url, transcript')
      .eq('id', job.simulation_id)
      .single();

    if (simErr) throw simErr;
    if (!sim || !sim.video_url) throw new Error('No video_url on simulation');

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

    console.log('Extracting frames with ffmpeg…');
    // 1 frame per second is enough for a visual overview
    await execPromise(`ffmpeg -i ${videoPath} -vf "fps=1" ${framesDir}/frame-%02d.jpg`);

    const frameFiles = fs
      .readdirSync(framesDir)
      .filter((f) => f.toLowerCase().endsWith('.jpg'));

    const frameUrls = [];
    console.log('Uploading', frameFiles.length, 'frames to storage…');

    for (const file of frameFiles) {
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
        frameUrls.push(publicData.publicUrl);
      }
    }

    // --- OpenAI visual + editing/storytelling analysis ----------------------

    const visualPrompt = [
      'You are a short-form video creative strategist.',
      'You will receive:',
      '- A set of frame image URLs extracted from a short-form video',
      '- The full transcript of the video',
      '',
      'You must visually and narratively analyze the content and return ONLY valid JSON with this exact schema:',
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
      '  "storytelling_insights": {',
      '    "what_worked": [',
      '      "bullet about strong storytelling element",',
      '      "another bullet about what was effective"',
      '    ],',
      '    "what_to_improve": [',
      '      "bullet about a confusing / weak storytelling choice",',
      '      "another bullet about hook, stakes, clarity, etc."',
      '    ],',
      '    "key_changes": [',
      '      "very concrete, tactical change that would improve narrative clarity or hook",',
      '      "another concrete, tactical storytelling change"',
      '    ]',
      '  },',
      '',
      '  "editing_style_insights": {',
      '    "what_worked": [',
      '      "bullet about pacing, transitions, cuts, overlays, visual rhythm that worked",',
      '      "another bullet that is useful for an editor"',
      '    ],',
      '    "what_to_improve": [',
      '      "bullet about pacing issues, dead air, janky cuts, lack of B-roll, etc.",',
      '      "another bullet that an editor can act on"',
      '    ],',
      '    "key_changes": [',
      '      "very concrete editing tweak (e.g. \\"cut this section shorter\\", \\"add B-roll over this line\\")",',
      '      "another specific change an editor could implement in a single edit pass"',
      '    ]',
      '  }',
      '}',
      '',
      'Make sure every array has 3–4 short, punchy bullets that a creator would actually find useful.',
      '',
      'Frames:',
      frameUrls.join('\n'),
      '',
      'Transcript:',
      sim.transcript || '(no transcript available)',
    ].join('\n');

    console.log('Calling OpenAI for visual + storytelling/editing analysis…');

    const completion = await openai.chat.completions.create({
      model: 'gpt-4.1',
      response_format: { type: 'json_object' },
      messages: [
        {
          role: 'system',
          content:
            'You are a visual + narrative strategist for TikTok/Reels shorts. Always return strict JSON.',
        },
        { role: 'user', content: visualPrompt },
      ],
    });

    const content = completion.choices?.[0]?.message?.content;
    if (!content) throw new Error('Empty visual analysis response');

    const visualAnalysis = JSON.parse(content);

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
