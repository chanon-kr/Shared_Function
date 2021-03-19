import pandas as pd
from sklearn.preprocessing import LabelEncoder

def create_encoding(df_in , col_in, folder_in) :
    if not os.path.exists(folder_in) : os.mkdir(folder_in)
    for i_in in col_in :
        en_encoder = LabelEncoder()
        en_encoder.fit(df_in[i_in].astype('str').astype('category'))
        dump(en_encoder, '{}/en__{}__.pkl'.format(folder_in , i_in))
        del en_encoder

def encode_col(df_in , col_in , folder_in , debug = False) :
    for i_in in col_in :
        en_encoder = load('{}/en__{}__.pkl'.format(folder_in, i_in))
        all_class = list(en_encoder.classes_)
        df_in[i_in] = df_in[i_in].astype('str').astype('category').apply(lambda j_in : 
                                                             en_encoder.transform([j_in])[0] if j_in in all_class 
                                                             else len(all_class) )
        if debug : print(i_in , ':' , len(all_class) ,'Classes')
    return df_in

def decode_col(df_in , col_in , folder_in , debug = False) :
    for i_in in col_in :
        en_encoder = load('{}/en__{}__.pkl'.format(folder_in, i_in))
        all_class = list(en_encoder.classes_)
        df_in[i_in] = df_in[i_in].astype('int').apply(lambda j_in : 
                                                             en_encoder.inverse_transform([j_in])[0] if j_in < len(all_class) 
                                                             else 'unknown' )
        if debug : print(i_in , ':' , len(all_class) ,'Classes')
    return df_in

