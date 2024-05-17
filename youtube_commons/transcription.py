import logging
import os

from faster_whisper import WhisperModel


logger = logging.getLogger(__name__)


class WhisperTranscriber:
    def __init__(self, output_dir, model_size="small", device="cpu", compute_type="int8"):

        self.model = WhisperModel(model_size, device=device, compute_type=compute_type)

    
    def transcribe(self, audio_file):
        segments, info = self.model.transcribe(audio_file, vad_filter=True, beam_size=5)
        return "".join([segment.text for segment in segments])
