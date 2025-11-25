import { createClient } from '@supabase/supabase-js'
import { exec } from 'child_process'
import fs from 'fs'
import fetch from 'node-fetch'
import OpenAI from 'openai'

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

function execPromise(cmd) {
  return new Promise((resolve, reject) => {
    exec(cmd, (err) => {
      if (err) reject(err)
      else resolve()
    })
  })
}

async function processVisualJob() {
  const { data: jobs, error } = await supabase
    .from('visual_jobs')
    .select('id, simulation_id')
    .eq('status', 'pending')
    .limit(1)

  if (error || !jobs || jobs.length === 0) return

  const job = jobs[0]

  await supabase
    .from('visual_jobs')
    .update({ status: 'processing' })
    .eq('id', job.id)

  const { data: sim } = await supabase
    .from('simulations')
    .select('video_url, transcript')
    .eq('id', job.simulation_id)
    .single()

  if (!sim?.video_url) throw new Error('No video_url on simulation')

  const videoRes = await fetch(sim.video_url)
  const arrayBuf = await videoRes.arrayBuffer()
  const videoPath = `/tmp/${job.id}.mp4`
  fs.writeFileSync(videoPath, Buffer.from(arrayBuf))

  const framesDir = `/tmp/frames-${job.id}`
  fs.mkdirSync(framesDir, { recursive: true })

  await execPromise(`ffmpeg -i ${videoPath} -vf "fps=1" ${framesDir}/frame-%02d.jpg`)

  const frameFiles = fs.readdirSync(framesDir).filter(f => f.endsWith('.jpg'))
  const frameUrls = []

  for (const file of frameFiles) {
    const full = `${framesDir}/${file}`
    const buf = fs.readFileSync(full)
    const uploadPath = `${job.simulation_id}/${job.id}/${file}`

    await supabase.storage
      .from('video-frames')
      .upload(uploadPath, buf, { contentType: 'image/jpeg', upsert: true })

    const { data } = supabase.storage
      .from('video-frames')
      .getPublicUrl(uploadPath)

    frameUrls.push(data.publicUrl)
  }

  const visualPrompt = `
You are analyzing short-form social video frames.

Here are the frame image URLs:
${frameUrls.join('\n')}

Transcript (may be empty):
${sim.transcript || ''}

Return ONLY valid JSON following this schema exactly:

{
  "visual_style": "overall aesthetic and editing style",
  "pacing_description": "how fast cuts occur, motion level, energy",
  "on_screen_text_usage": "describe captions or text overlays",
  "aesthetic_tags": ["short tags describing aesthetic vibe"],
  
  "environment_analysis": {
    "environment_type": "indoor | outdoor | studio | room | nature | urban | unknown",
    "environment_description": "detailed description of location and scenery",
    "foreground_objects": ["object1", "object2"],
    "background_elements": ["element1", "element2"]
  },

  "motion_analysis": {
    "is_static": true,
    "camera_movement": "none | panning | handheld | zoom | shaky",
    "subject_movement": "none | low | medium | high"
  },

  "scene_summary": "holistic summary of what visually happens in the video",

  "video_type_inference": "guess the type of video: vlog, b-roll, meme, tutorial, aesthetic edit, storytime, promo, educational, etc.",

  "quality_assessment": {
    "resolution_ok": true,
    "lighting_ok": "good | okay | poor",
    "edit_quality": "simple | basic | advanced | chaotic"
  }
}

Rules:
- You MUST output valid JSON only.
- Use BOTH the frames and transcript to infer context.
- If frames are static, explicitly state that the video is static.
- If outdoor scenery is visible, describe it accurately.
`.trim()

`

  const completion = await openai.chat.completions.create({
    model: 'gpt-4.1',
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: 'You analyze short-form video frames visually.' },
      { role: 'user', content: visualPrompt }
    ]
  })

  const visualAnalysis = JSON.parse(completion.choices[0].message.content)

  await supabase
    .from('visual_jobs')
    .update({
      status: 'complete',
      frames: frameUrls,
      visual_analysis: visualAnalysis
    })
    .eq('id', job.id)

  await supabase
    .from('simulations')
    .update({ visual_analysis: visualAnalysis })
    .eq('id', job.simulation_id)
}

async function mainLoop() {
  console.log('Visual worker running…')
  while (true) {
    try {
      await processVisualJob()
    } catch (err) {
      console.error('Worker error:', err)
    }
    await new Promise(r => setTimeout(r, 3000))
  }
}

mainLoop()
