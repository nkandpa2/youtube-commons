import logging

import whisperx


logger = logging.getLogger(__name__)


class WhisperXTranscriber:
    def __init__(self, transcription_model, hf_auth_token, batch_size=16, compute_type="float16", device="cuda"):
        self.transcription_model = whisperx.load_model(transcription_model, device, compute_type=compute_type)
        self.alignment_models = {}
        self.diarization_model = whisperx.DiarizationPipeline(use_auth_token=hf_auth_token, device=device)
        self.batch_size = batch_size
        self.device = device
    
    def format_transcript(self, transcription_result):
        if not any(["speaker" in segment.keys() for segment in transcription_result["segments"]]):
            return "".join([segment["text"] for segment in transcription_result["segments"]])
        
        speakers = set([segment["speaker"] for segment in transcription_result["segments"] if "speaker" in segment])
        if len(speakers) == 1:
            return " ".join([segment["text"] for segment in transcription_result["segments"]])
            
        curr_speaker = None
        text = ""
        for segment in transcription_result["segments"]:
            speaker = segment.get("speaker", curr_speaker)
            if speaker != curr_speaker:
                text += f"{speaker}: " if len(text) == 0 else f"\n{speaker}: "
                curr_speaker = speaker
            text += " " + segment["text"]
        
        return text

    def transcribe(self, audio_file):
        audio = whisperx.load_audio(audio_file)

        transcription_result = self.transcription_model.transcribe(audio, batch_size=self.batch_size)
        detected_language = transcription_result["language"]
        logging.debug(f"Detected language {detected_language} for file {audio_file}")

        if detected_language in self.alignment_models:
            alignment_model, metadata = self.alignment_models[detected_language]
        else:
            try:
                alignment_model, metadata = whisperx.load_align_model(language_code=detected_language, device=self.device)
                self.alignment_models[detected_language] = (alignment_model, metadata)
                logging.debug(f"Loaded new alignment model for language {detected_language}")
            except ValueError:
                logging.debug(f"Failed to load new alignment model for language {detected_language}")
                alignment_model, metadata = None, None
        
        # Only attempt alignment and diarization if an alignment model for this audio's language is available
        if alignment_model is not None:
            transcription_result = whisperx.align(transcription_result["segments"], alignment_model, metadata, audio, self.device, return_char_alignments=False)
            diarize_segments = self.diarization_model(audio)
            transcription_result = whisperx.assign_word_speakers(diarize_segments, transcription_result)
        
        return self.format_transcript(transcription_result)
