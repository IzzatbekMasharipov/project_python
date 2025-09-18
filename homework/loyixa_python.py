import json
import pandas as pd
import uuid

ds=pd.read_csv("d:\\power BI\\loyixa\\raw_data.csv")

def ustun_json(raw):
    try:
        return json.loads(raw)
    except:
        return None
ds['new_ustun']=ds['raw_content'].apply(ustun_json)

#yangi ustunlar ##########################
def calendar(raw):
    if raw:
        return raw.get("calendar_id")
    
    return None
ds['calendar_id']=ds['new_ustun'].apply(calendar)

###############################################
ds['new_ustun'][0]
def audio_url(raw):
    if raw:
        return raw.get("audio_url")
    return None
ds['raw_audio_url']=ds['new_ustun'].apply(audio_url)
#################################################

def transcript_url(raw):
    if raw:
        return raw.get('transcript_url')
    return None
ds['raw_transcript_url']=ds['new_ustun'].apply(transcript_url)
####################################################
def raw_id(raw):
    if raw:
        return raw.get("id")
    return None
ds['raw_id']=ds['new_ustun'].apply(raw_id)
##################################################
def time(raw):
    if raw:
        return raw.get("dateString")
    return None
ds['datetime_id']=ds['new_ustun'].apply(time)
###############################################
def title(raw):
    if raw:
        return raw.get('title')
    return None
ds['raw_title']=ds['new_ustun'].apply(title)
##############################################
def duration(raw):
    if raw:
        return raw.get('duration')
    return None
ds['raw_duration']=ds['new_ustun'].apply(duration)
#########################################
def vidio(raw):
    if raw:
        return raw.get("video_url")
    return None
ds['raw_video_url']=ds['new_ustun'].apply(vidio)
#################################################
def speaker(raw):
    if raw:
        return raw.get("speakers")
    return None
ds['speakers']=ds['new_ustun'].apply(speaker)
##################################################
def participant(raw):
    if raw:
        return raw.get("participants")
    return None
ds['participants']=ds['new_ustun'].apply(participant)
#############################################
def meet_email(raw):
    if raw:
        return raw.get("meeting_attendees")
    return None
ds['meeting_attendees']=ds['new_ustun'].apply(meet_email)
#############################################
def hostemail(raw):
    if raw:
        return raw.get("host_email")
    return None
ds['host_email']=ds['new_ustun'].apply(hostemail)
##############################################
def organizeremail(raw):
    if raw:
        return raw.get('organizer_email')
    return None
ds['organizer_email']=ds['new_ustun'].apply(organizeremail)
#########################################################
#                          # Dim fayllar
Dim_comm_type=ds[['comm_type']].drop_duplicates().reset_index(drop=True)
Dim_comm_type['comm_type_id']=Dim_comm_type.index+1
############################################
Dim_subject=ds[['subject']].drop_duplicates().reset_index(drop=True)
Dim_subject['subject_id']=Dim_subject.index+1
###########################################
Dim_calendar=ds[['calendar_id']].drop_duplicates().reset_index(drop=True)
Dim_calendar.columns=['raw_calendar_id']
Dim_calendar['calendar_id']=Dim_calendar.index+1
############################################
dim_audio=ds[['raw_audio_url']].drop_duplicates().reset_index(drop=True)
dim_audio['audio_id']=dim_audio.index+1
#############################################
dim_transcript=ds[['raw_transcript_url']].reset_index(drop=True)
dim_transcript['transcript_id']=dim_transcript.index+1
dim_transcript.dropna(inplace=True)
##############################################
dim_vidio=ds[['raw_video_url']].reset_index(drop=True)
dim_vidio['video_id']=dim_audio.index+1
#####################################


dim_user=[]
for i in range(len(ds['speakers'])):
    dim_user+=(ds['speakers'][i]+ds['participants'][i])
    dim_user.append(ds['host_email'][i])
    dim_user.append(ds['organizer_email'][i])
user_id=[]
dim_user=set(dim_user)
dim_user=list(dim_user)
for i in range(len(dim_user)):
    user_id.append(str(uuid.uuid4()))
dim_user=pd.DataFrame({"email":dim_user,"user_id":user_id})
dim_user
##################################################################
#Fakt jadval yaratish merge
ds=ds.merge(Dim_comm_type,on='comm_type',how='left')
ds=ds.merge(Dim_subject,on='subject',how='left')
ds=ds.merge(Dim_calendar,left_on='calendar_id',right_on="raw_calendar_id",how='left')
ds=ds.merge(dim_audio,on='raw_audio_url',how='left')
ds=ds.merge(dim_vidio,on='raw_video_url',how='left')
ds=ds.merge(dim_transcript,on='raw_transcript_url',how='left')


fact_communication=ds[['id','raw_id','source_id','comm_type_id_y',
'subject_id','calendar_id_y','audio_id','video_id','transcript_id_y',
'datetime_id','ingested_at','processed_at','is_processed',
'raw_title','raw_duration']]
##################################################################
#Bridge jadvali 
bridge_rows = []

for i, row in ds.iterrows():
    comm_id = row['id']
    
    
    for email in row['speakers'] or []:
        uid = dim_user.loc[dim_user['email'] == email, 'user_id'].values[0]
        bridge_rows.append({
            'comm_id': comm_id,
            'user_id': uid,
            'isSpeaker': True,
            'isParticipant': False,
            'isAttendee': False,
            'isOrganiser': False
        })
    
    
    for email in row['participants'] or []:
        uid = dim_user.loc[dim_user['email'] == email, 'user_id'].values[0]
        bridge_rows.append({
            'comm_id': comm_id,
            'user_id': uid,
            'isSpeaker': False,
            'isParticipant': True,
            'isAttendee': False,
            'isOrganiser': False
        })
    
    
    for email in row['meeting_attendees'] or []:
        uid = dim_user.loc[dim_user['email'] == email, 'user_id']
        if not uid.empty:
          uid = uid.values[0]
        else:
         uid = None

        bridge_rows.append({
            'comm_id': comm_id,
            'user_id': uid,
            'isSpeaker': False,
            'isParticipant': False,
            'isAttendee': True,
            'isOrganiser': False
        })
    
    
    if row['organizer_email']:
        uid = dim_user.loc[dim_user['email'] == row['organizer_email'], 'user_id'].values[0]
        bridge_rows.append({
            'comm_id': comm_id,
            'user_id': uid,
            'isSpeaker': False,
            'isParticipant': False,
            'isAttendee': False,
            'isOrganiser': True
        })


bridge_comm_user = pd.DataFrame(bridge_rows)
bridge_comm_user.dropna(inplace=True)

###############################################
#exselga yozish


with pd.ExcelWriter("final_model.xlsx") as writer:
 
    Dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
    Dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    Dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
    dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
    dim_vidio.to_excel(writer, sheet_name="dim_video", index=False)
    dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
    bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)


