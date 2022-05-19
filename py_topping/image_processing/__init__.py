from tensorflow.keras.models import load_model
from tensorflow import device, reduce_mean
from tensorflow.image import ssim
import numpy as np
import cv2

def SSIMLoss(y_true, y_pred):
    return 1 - reduce_mean(ssim(y_true, y_pred,1.0))

class lazy_ImageProcessing :
    """Intented to use with Google TeachableAI's Image Classification and Auto-Encoder"""
    def __init__(self , model_path, model_type = None, class_list = None, loss_function = None
                 , custom_objects = None
                 , size = (224,224,3), cvtColor = None
                 , normalize_type = np.float32
                 , normalize_divide = 127, normalize_add = -1
                 , device = 'cpu') :
        if model_type == 'image' : 
            self.class_list, self.model_type = class_list, 'image'
        elif model_type == 'autoencoder' : 
            self.loss_function, self.model_type = loss_function, 'autoencoder'
        # Load Model
        if custom_objects == None :
            self.model = load_model(model_path)
        else :
            self.model = load_model(model_path, custom_objects= custom_objects)
        self.cvtColor = cvtColor
        self.loss_function, self.size = loss_function, size
        self.normalize_type = normalize_type
        self.normalize_divide, self.normalize_add = normalize_divide, normalize_add
        self.device = device

    def format_image(self, image) :
        image_array, color_ = image[:], 3
        # Recolor
        if self.cvtColor != None : 
            image_array = cv2.cvtColor(image_array, self.cvtColor)
            if (cv2.COLOR_BGR2GRAY == self.cvtColor)|(cv2.COLOR_RGB2GRAY == self.cvtColor) :
                color_ = 1
        # Reshape
        image_array = cv2.resize(image_array, (self.size[0], self.size[1]))
        image_array = np.reshape(image_array , (1,self.size[0], self.size[1], color_))
        # Normalize the image
        normalized_image_array = (image_array.astype(self.normalize_type) / self.normalize_divide) + self.normalize_add
        return normalized_image_array
    
    def predict(self, image, output_type = 'image') :
        with device('/{}:0'.format(self.device)): 
            formatted_image = self.format_image(image)
            prediction = self.model.predict(formatted_image)
        if self.model_type == 'image' : return dict(zip(self.class_list, prediction.flatten().tolist())) 
        elif self.model_type == 'autoencoder' :
            if output_type == 'image' : return cv2.cvtColor(prediction[0], cv2.COLOR_GRAY2RGB)
            elif output_type == 'loss' : 
                return self.loss_function(prediction, formatted_image)
            elif output_type == 'both' : 
                return { 'image' : cv2.cvtColor(prediction[0], cv2.COLOR_GRAY2RGB)
                        ,'loss' : self.loss_function(prediction, formatted_image)}