package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os/exec"
	"time"
)

const (
	voiceAnalysisSampleRate = 8000
	voiceWaveformBuckets    = 64
	voiceWaveformMaxValue   = 1024
	voiceAnalysisTimeout    = 30 * time.Second
)

// analyzeAudioPCM computes duration in milliseconds and an MSC3246/MSC1767 waveform
// (amplitudes in [0, 1024]) from mono signed 16-bit little-endian PCM bytes.
func analyzeAudioPCM(pcm []byte, sampleRate int) (durationMs int, waveform []int) {
	if sampleRate <= 0 || len(pcm) < 2 {
		return 0, nil
	}
	totalSamples := len(pcm) / 2
	durationMs = int(int64(totalSamples) * 1000 / int64(sampleRate))

	buckets := voiceWaveformBuckets
	if totalSamples < buckets {
		buckets = totalSamples
	}
	if buckets <= 0 {
		return durationMs, nil
	}

	rawRMS := make([]float64, buckets)
	var maxRMS float64
	for b := 0; b < buckets; b++ {
		start := b * totalSamples / buckets
		end := (b + 1) * totalSamples / buckets
		if end <= start {
			end = start + 1
		}
		if end > totalSamples {
			end = totalSamples
		}
		var sumSq float64
		count := end - start
		for i := start; i < end; i++ {
			sample := int16(binary.LittleEndian.Uint16(pcm[i*2 : i*2+2]))
			f := float64(sample)
			sumSq += f * f
		}
		rms := math.Sqrt(sumSq / float64(count))
		rawRMS[b] = rms
		if rms > maxRMS {
			maxRMS = rms
		}
	}

	waveform = make([]int, buckets)
	if maxRMS == 0 {
		return durationMs, waveform
	}

	// Normalize against the louder of peak RMS or a noise floor (~5% of full scale)
	// so quiet speech still renders a readable waveform without amplifying near-silence.
	const minNormFloor = 1638.4 // 0.05 * 32768
	scale := maxRMS
	if scale < minNormFloor {
		scale = minNormFloor
	}
	for b, rms := range rawRMS {
		val := int(math.Round((rms / scale) * voiceWaveformMaxValue))
		if val < 0 {
			val = 0
		} else if val > voiceWaveformMaxValue {
			val = voiceWaveformMaxValue
		}
		waveform[b] = val
	}
	return durationMs, waveform
}

// extractVoiceMetadata decodes audio bytes using ffmpeg into mono 8kHz s16le PCM
// to extract duration (ms) and an MSC1767/MSC3246 waveform array.
func extractVoiceMetadata(ctx context.Context, audioData []byte) (int, []int, error) {
	if len(audioData) == 0 {
		return 0, nil, nil
	}
	cmdCtx, cancel := context.WithTimeout(ctx, voiceAnalysisTimeout)
	defer cancel()

	cmd := exec.CommandContext(
		cmdCtx,
		"ffmpeg",
		"-hide_banner",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-f", "s16le",
		"-ac", "1",
		"-ar", fmt.Sprintf("%d", voiceAnalysisSampleRate),
		"-vn",
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(audioData)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return 0, nil, fmt.Errorf("ffmpeg audio analysis failed: %w (%s)", err, bytes.TrimSpace(stderr.Bytes()))
		}
		return 0, nil, fmt.Errorf("ffmpeg audio analysis failed: %w", err)
	}

	durationMs, waveform := analyzeAudioPCM(stdout.Bytes(), voiceAnalysisSampleRate)
	return durationMs, waveform, nil
}
