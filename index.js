import { createClient } from '@supabase/supabase-js'
import { exec } from 'child_process'
import fs from 'fs'
import fetch from 'node-fetch'
import OpenAI from 'openai'

const supabaseUrl = process.env.SUPABASE_URL
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY
const openaiKey = process.env.OPENAI_API_KEY

if (!supabaseUrl || !supabaseServiceKey || !openaiKey) {
  console.error('Missing env vars SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / OPENAI_API_KEY')
  process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseServiceKey)
const openai = new OpenAI({ apiKey: openaiKey })

function execPromise(cmd) {
  return new Promise((resolve, reject) => {
    exec(cmd, (err) => {
      if (err) reject(err)
      else resolve()
    })
  })
}

async function processVisualJob() {
  console.log('Checking for pending/failed visual_jobs...')

  // 🔥 IMPORTANT CHANGE: pick up BOTH pending + failed
  const { data: jobs, error } = await supabase
    .from('visual_jobs')
    .select('id, simulation_id, status')
    .in('status', ['pending', 'failed'])
    .limit(1)

  if (error) {
    console.error('visual_jobs select error:', error)
    return
  }

  if (!jobs || jobs.length === 0) {
    console.log('No pending/failed jobs.')
    return
  }

  const job = jobs[0]
  console.log(`Processing job ${job.id} (status=${job.status}) for simulation ${job.simulation_id}`)

  // mark as processing
  await supabase
    .from('visual_jobs')
    .update({ status: 'processing', error_message: null })
    .eq('id', job.id)

  try {
    const { data: sim, error: simErr } = await supabase
      .from('simulations')
      .select('video_url, transcript')
      .eq('id', job.simulation_id)
      .single()

    if (simErr) throw simErr
    if (!sim?.video_url) throw new Error('No video_url on simulation')

    console.log('Downloading video:', sim.video_url)

    const videoRes = await fetch(sim.video_url)
    if (!videoRes.ok) {
      throw new Error(`Failed to fetch video: ${videoRes.status} ${videoRes.statusText}`)
    }

    const arrayBuf = await videoRes.arrayBuffer()
    const videoPath = `/tmp/${job.id}.mp4`
    fs.writeFileSync(videoPath, Buffer.from(arrayBuf))

    const framesDir = `/tmp/frames-${job.id}`
    fs.mkdirSync(framesDir, { recursive: true })

    console.log('Extracting frames with ffmpeg...')
    await execPromise(`ffmpeg -i ${videoPath} -vf "fps=1" ${framesDir}/frame-%02d.jpg`)

    const frameFiles = fs.readdirSync(framesDir).filter((f) => f.endsWith('.jpg'))
    const frameUrls = []

    console.log('Uploading', frameFiles.length, 'frames to storage...')

    for (const file of frameFiles) {
      const full = `${framesDir}/${file}`
      const buf = fs.readFileSync(full)
      const uploadPath = `${job.simulation_id}/${job.id}/${file}`

      const { error: uploadErr } = await supabase.storage
        .from('video-frames')
        .upload(uploadPath, buf, { contentType: 'image/jpeg', upsert: true })

      if (uploadErr) throw uploadErr

      const { data } = supabase.storage
        .from('video-frames')
        .getPublicUrl(uploadPath)

      frameUrls.push(data.publicUrl)
    }

    const visualPrompt = [
      'Frames:',
      frameUrls.join('\n'),
      '',
      'Transcript:',
      sim.transcript || '',
      '',
      'Return ONLY JSON with this schema:',
      '{',
      '  "visual_style": "string",',
      '  "pacing_description": "string",',
      '  "on_screen_text_usage": "string",',
      '  "aesthetic_tags": ["string"],',
      '  "scene_summary": "string",',
      '  "quality_assessment": {',
      '    "resolution_ok": true,',
      '    "lighting_ok": "good | okay | poor",',
      '    "edit_quality": "simple | basic | advanced | chaotic"',
      '  }',
      '}'
    ].join('\n')

    console.log('Calling OpenAI for visual analysis...')

    const completion = await openai.chat.completions.create({
      model: 'gpt-4.1',
      response_format: { type: 'json_object' },
      messages: [
        { role: 'system', content: 'You analyze short-form video frames visually.' },
        { role: 'user', content: visualPrompt }
      ]
    })

    const content = completion.choices[0].message.content
    if (!content) throw new Error('Empty visual analysis response')

    const visualAnalysis = JSON.parse(content)

    console.log('Saving visual analysis to Supabase...')

    await supabase
      .from('visual_jobs')
      .update({
        status: 'complete',
        frames: frameUrls,
        visual_analysis: visualAnalysis,
        error_message: null
      })
      .eq('id', job.id)

    await supabase
      .from('simulations')
      .update({ visual_analysis: visualAnalysis })
      .eq('id', job.simulation_id)

    console.log('Job completed:', job.id)
  } catch (err) {
    console.error('processVisualJob error:', err)

    await supabase
      .from('visual_jobs')
      .update({
        status: 'failed',
        error_message: String(err)
      })
      .eq('id', job.id)
  }
}

async function mainLoop() {
  console.log('Visual worker running…')
  while (true) {
    try {
      await processVisualJob()
    } catch (err) {
      console.error('Worker error (outer):', err)
    }
    await new Promise((r) => setTimeout(r, 3000))
  }
}

mainLoop()
