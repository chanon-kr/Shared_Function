import tensorflow as tf
import matplotlib.pyplot as plt

def lazy_dnn(train_in, target_in , num_col_in = [], cat_col_in = [] , node_in = [16,4]
              , val_in = None , train_batch = 10 , val_batch = 10 , epochs = 10 , normalize = True
              , embedded_size = 16 , drop_out_ratio = 0.1, patience_in = 10
              , amplify_in = 1,  optimize_in = 'adam', loss_in = 'mse' , debug = False) :
    
    input_num_list = []
    input_cat_list = []
    normalize_list = []
    embedded_list = []

    labels = train_in.copy().pop(target_in)
    train_ds = tf.data.Dataset.from_tensor_slices((dict(train_in), labels))
    train_ds = train_ds.shuffle(buffer_size=len(train_in))
    train_ds = train_ds.batch(train_batch)
    
    if "<class 'pandas.core.frame.DataFrame'>" == str(type(val_in)) :
        labels = val_in.copy().pop(target_in)
        val_ds = tf.data.Dataset.from_tensor_slices((dict(val_in), labels))
        val_ds = val_ds.shuffle(buffer_size=len(val_in))
        val_ds = val_ds.batch(val_batch)
    else : val_ds = None
    
    for i_in in num_col_in :
        j_in = tf.keras.Input(shape=(1,), name=i_in)
        input_num_list.append(j_in)
        normalize_list.append(tf.keras.layers.BatchNormalization(
                                                    name= str(i_in) + '_normalize',
                                                )(j_in))

    if not normalize : normalize_list = input_num_list

    for i_in in cat_col_in :
        j_in = tf.keras.Input(shape=(1,), name=i_in)
        input_cat_list.append(j_in)
        embedded_list.append(tf.keras.layers.Embedding( train_in[i_in].astype(int).max() + 2, embedded_size, 
                                input_length=1, name= str(i_in) + '_embedding')(j_in))
        
    if len(embedded_list) > 0 : 
        if len(embedded_list) == 1 : all_cat_fea = embedded_list[0]
        else : all_cat_fea = tf.keras.layers.concatenate(embedded_list)
        all_cat_fea = tf.keras.layers.Dropout(drop_out_ratio)(all_cat_fea)
        flat_layer = tf.keras.layers.Flatten()(all_cat_fea)
        if len(normalize_list) > 0 :
            flat_layer = tf.keras.layers.concatenate(normalize_list +  [flat_layer])
    else : 
        if len(normalize_list) == 1 : flat_layer = normalize_list[0]
        else : flat_layer = tf.keras.layers.concatenate(normalize_list)
    
    count_in = 0
    for i_in in node_in :
        if count_in == 0 :
            con_layer = tf.keras.layers.Dense(i_in, activation="relu")(flat_layer)
            count_in += 1
        else : con_layer = tf.keras.layers.Dense(i_in, activation="relu")(con_layer)
            
    con_layer = tf.keras.layers.Dense(1)(con_layer)
    out = tf.keras.layers.Lambda(lambda x : x*amplify_in )(con_layer)
    model_out = tf.keras.Model(input_num_list + input_cat_list, out)
    
    if debug : print(model_out.summary())
    model_out.compile(optimize_in,loss=loss_in)
    
    if "<class 'pandas.core.frame.DataFrame'>" == str(type(val_in)) : monitor_loss = 'val_loss'
    else : monitor_loss = 'loss'
        
    callbacks = tf.keras.callbacks.EarlyStopping(
                                                monitor = monitor_loss,  patience = patience_in, verbose=0,
                                                mode='auto', restore_best_weights=True
                                                )

    if "<class 'pandas.core.frame.DataFrame'>" == str(type(val_in)) :
        history = model_out.fit(train_ds , validation_data = val_ds , epochs=epochs , callbacks = [callbacks])
    else : history = model_out.fit(train_ds , epochs=epochs , callbacks = [callbacks])
    
    if debug :
        fig, ax = plt.subplots(figsize = (10,6))
        ax.plot(history.history['loss'], color = 'b')
        if "<class 'pandas.core.frame.DataFrame'>" == str(type(val_in)) : ax.plot(history.history['val_loss'], color = 'r')
        plt.show()
        
    return model_out