import io

import cv2
import numpy as np
import torch


class Yolo:

    def __init__(self):
        self.model = torch.hub.load("ultralytics/yolov5",
                                    "custom", path='best.pt',
                                    force_reload=True)

    def _predict(self, image):
        im = cv2.imread(image)
        outputs = self.model(im)
        buffer = outputs.ims[0]
        bio = io.BytesIO(buffer)
        bio.name = "image.jpeg"
        bio.seek(0)
        text = sorted(set(np.array(outputs)), reverse=True)
        return bio, text

    def __call__(self, image: str):
        answer = self._predict(image)
        return answer
