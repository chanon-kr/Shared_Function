from keras.models import load_model
import numpy as np
import cv2
from tensorflow import device


class lazy_ImageClassification :
    """Intented to use with Google's TeachableAI"""
    def __init__(self , model_path, class_list, size = (224,224), normalize_type = np.float32, normalize_divide = 127, normalize_add = -1, device = 'cpu') :
        self.model = load_model(model_path)
        self.class_list, self.size = class_list, size
        self.normalize_type = normalize_type
        self.normalize_divide, self.normalize_add = normalize_divide, normalize_add
        self.device = device

    def format_image(self, image) :
        # Reshape
        image_array = cv2.resize(image, (self.size))
        image_array = np.reshape(image_array , (1,self.size[0], self.size[1], 3))
        # Normalize the image
        normalized_image_array = (image_array.astype(self.normalize_type) / self.normalize_divide) + self.normalize_add
        return normalized_image_array
    
    def predict(self, image) :
        with device('/{}:0'.format(self.device)): 
            formatted_image = self.format_image(image)
            prediction = self.model.predict(formatted_image).flatten()
        return dict(zip(self.class_list, prediction.tolist()))