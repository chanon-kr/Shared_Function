
from datetime import datetime
import os, traceback
import papermill as pm
import pandas as pd
from py_topping.general_use import email_sender

def run_script(script_list , out_folder = '', out_prefix = None): #, email_sender, email_sendto,email_subject ,run_output , ) :
  logs_out = []

  # Create Directory if not exists
  if (not os.path.exists(out_folder)) & (out_folder != '') :
    os.mkdir(out_folder)

  # Execute Python Part
  for i in script_list :
    log_out = []
    log_out.append(str(datetime.now()).split('.')[0])
    log_out.append(i)
    try :
        if i.split('.')[-1] == 'ipynb' : 
            if out_prefix == None : prefix = datetime.now().strftime('%Y_%m_%d_%H_%M_')
            else : prefix = out_prefix
            out_name = str(out_folder) + '/' + prefix + i.split('\\')[-1].split('/')[-1]
            log_out.append(out_name)
            pm.execute_notebook(i,out_name)
        elif i.split('.')[-1] == 'py' :
            log_out.append(None)
            with open(i, encoding="utf8",newline='') as f :
                exec(f.read())
        else : 
            log_out.append(None)
            raise Exception("File Type Not Match, Please use .py or .ipynb file")
        log_out.append('OK')
    except  :
        log_out.append(str(traceback.format_exc()))

    log_out.append(str(datetime.now()).split('.')[0])
    logs_out.append(log_out)
  
  return pd.DataFrame(logs_out, columns = ['start','script','notebook_out','run_result','end'])

def run_pipeline(script_list, out_folder = '', out_prefix = None, email_dict = {} , sending = False
                    , only_error = False, notebook_attached = False, attached_only_error = True, attached_log = False, log_sql = None) :

    if sending :
        if (['user' , 'password', 'server' ,'sendto','subject'].sort() != list(email_dict.keys()).sort()) or (email_dict == {}) :
            raise Exception("email_dict parameter must have 'user' , 'password' , 'server' , 'sendto' , 'subject' ")

        run_output = 'Start Job at ' + str(datetime.now()) + '<br>' + '<br>'

    run_log = run_script(script_list , out_folder, out_prefix )
    out_log = run_log.copy()

    if log_sql != None : 
        if ['job_name','table_name' , 'da_tran_SQL'].sort() != list(log_sql.keys()).sort() :
            raise Exception("log_sql parameter must have 'job_name','table_name' , 'da_tran_SQL' ")
        run_log['job_name'] = log_sql['job_name']
        run_log.astype('str').to_sql(log_sql['table_name'], con = log_sql['da_tran_SQL'].engine , if_exists = 'append', index = False)
        run_log = run_log.drop(['job_name'], axis = 1)

    if sending & only_error  : 
        if (run_log['run_result'] != 'OK').sum() == 0 : sending = False
        else : run_log = run_log[(run_log['run_result'] != 'OK')]

    if sending & notebook_attached : 
        if attached_only_error : 
            attached = run_log[(run_log['run_result'] != 'OK') & (run_log['notebook_out'].str.len() > 0)]['notebook_out'].unique()
        else : 
            attached = run_log[(run_log['notebook_out'].str.len() > 0)]['notebook_out'].unique()
        if len(attached) == 0 : attached = None
        else : attached = list(attached)
    else : attached = None
    
    if sending & attached_log :
        file_name = 'LOG_{}.csv'.format(datetime.now().strftime('%Y_%m_%d_%H_%M'))
        run_log.to_csv(file_name,index = False)
        if attached == None : attached = [file_name]
        else : attached.append(file_name)

    if sending :
        run_output += run_log[['script','start','end','run_result']].to_html(index = False)
        run_output = run_output.replace('\\n','<br>')
        email_subject = email_dict['subject'] + ' ' + str(datetime.now())
        em = email_sender(email_dict['user'] , email_dict['password'] , email_dict['server'])
        em.send(email_dict['sendto'] , email_subject , run_output , attachment= attached)

    if sending & attached_log : os.remove(file_name)

    return out_log  