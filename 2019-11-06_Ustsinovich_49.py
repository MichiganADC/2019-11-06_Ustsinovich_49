# 2019-11-06_Utsinovich_49.py


import numpy as np
import pandas as pd
import helpers
import config


################################################################################
# Extract
################################################################################


# MiNDSet Registry

fields_ms_raw = [
    'subject_id'
    , 'exam_date'
    , 'mri_completed'
    , 'mri_date'
    , 'blood_drawn'
    , 'blood_draw_date'
    , 'sample_given'
    , 'sample_date'
]
fields_ms = ','.join(fields_ms_raw)

df_ms = helpers.export_redcap_records(
    token=config.REDCAP_API_TOKEN_MINDSET
    , fields=fields_ms
)


# UMMAP - UDS 3

fields_u3_raw = [
    'ptid'
]
fields_u3 = ','.join(fields_u3_raw)

df_u3 = helpers.export_redcap_records(
    token=config.REDCAP_API_TOKEN_UDS3n
    , fields=fields_u3
)


# REVEAL Scan (Studies Database)

fields_rv_raw = [
    'subject_id'
    , 'study'
]
fields_rv = ','.join(fields_rv_raw)

df_rv = helpers.export_redcap_records(
    token=config.REDCAP_API_TOKEN_STUDIES
    , fields=fields_rv
)


# STIM

df_st = pd.read_excel("./data/STIM and UMMAP Co-Enrollment w PET Scan.xlsx")



################################################################################
# Transform
################################################################################


# MiNDSet Registry

df_ms.head()
df_ms.shape

# APOE
fields_u3_apoe = [
    'subject_id'
    , 'blood_drawn'
    , 'blood_draw_date'
    , 'sample_given'
    , 'sample_date'
]
df_ms_apoe = df_ms.loc[:, fields_u3_apoe]

# Filter for records that have:
#   EITHER (blood_drawn == 1 AND blood_drawn_date exists)
#   OR (sample_given == 1 AND sample_Date exists)
df_ms_apoe_cln = \
    df_ms_apoe[
        ((df_ms_apoe['blood_drawn'] == 1.0) &
         df_ms_apoe['blood_draw_date'].notna()) |
        ((df_ms_apoe['sample_given'] == 1.0) &
         df_ms_apoe['sample_date'].notna())
        ]
# Get unique IDs with APOE data
ids_ms_apoe = \
    pd.Series(df_ms_apoe_cln.loc[:, 'subject_id'].unique()).sort_values()

# MRI
fields_ms_mri = [
    'subject_id'
    , 'mri_completed'
    , 'mri_date'
]
df_ms_mri = df_ms.loc[:, fields_ms_mri]

# Filter for records that have:
#   mri_completed == 1 AND mri_date exists
df_ms_mri_cln = \
    df_ms_mri[
        (df_ms_mri['mri_completed'] == 1.0) & df_ms_mri['mri_date'].notna()
        ]

# Get unique IDs with MRI data
ids_ms_mri = \
    pd.Series(df_ms_mri_cln.loc[:, 'subject_id'].unique()).sort_values()
ids_ms_mri


# PET Scan - REVEAL Scan

df_rv.head()
df_rv.shape
df_rv.columns

df_rv_cln = df_rv.loc[:, fields_rv_raw]

# Filter for records that have:
#   study == 79 AND subject_id follows UMMAP ID pattern
df_rv_cln = \
    df_rv_cln[
        (df_rv['study'] == 79.0) &
        df_rv['subject_id'].str.contains(r'^UM\d{8}$')
    ]

# Get unique IDs in REVEAL Scan study
ids_rv = \
    pd.Series(df_rv_cln.loc[:, 'subject_id'].unique()).sort_values()


# PET Scan - STIM

df_st.head()
df_st.shape
df_st.columns

df_st_cln = df_st[df_st['PET Scan'] == "Yes"]

ids_st = \
    pd.Series(df_st_cln.loc[:, 'UMMAP ID'].unique()).sort_values()


# UMMAP IDs with APOE

ids_apoe = ids_ms_apoe.copy(deep=True)


# UMMAP IDs with Structural MRI
ids_mri = ids_ms_mri.copy(deep=True)


# UMMAP IDs with PET Scans

ids_pet = \
    pd.Series(pd.concat([ids_rv, ids_st], ignore_index=True).unique()).\
    sort_values()
ids_pet


####################################################
#  UMMAP Data (from MiNDSet)                       #
#    - age (from birth_date and exam_date)         #
#    - race_value: Black = 2; Hispanic = 4         #
#    - uds_dx: NL = 26; MCI = 1,2,31,34; AD = 3,4  #
####################################################

fields_um_raw = [
    'subject_id'
    , 'birth_date'
    , 'exam_date'
    , 'race_value'
    , 'uds_dx'
]
fields_um = ",".join(fields_um_raw)

df_um = helpers.export_redcap_records(
    token=config.REDCAP_API_TOKEN_MINDSET
    , fields=fields_um
)

df_um_cln = \
    df_um.loc[
        df_um['subject_id'].str.contains(r'^UM\d{8}$') &
        df_um['birth_date'].notnull() &
        df_um['exam_date'].notnull(),
        fields_um_raw
    ]

cols_date = ['birth_date', 'exam_date']

df_um_cln[cols_date] = df_um_cln[cols_date].apply(pd.to_datetime)
df_um_cln = df_um_cln[df_um_cln['exam_date'] >= pd.Timestamp("2017-03-01")]

df_um_cln_mut = df_um_cln.copy(deep=True)

df_um_cln_mut['age'] = \
    (df_um_cln_mut['exam_date'] - df_um_cln_mut['birth_date']).\
    astype('timedelta64[Y]')

# fields_um_keeper = ['subject_id', 'age', 'race_value', 'uds_dx']
# df_um_cln_mut = df_um_cln_mut[fields_um_keeper]


# APOE

df_apoe = \
    df_um_cln_mut[df_um_cln_mut['subject_id'].isin(ids_apoe)].\
    drop_duplicates(subset=['subject_id'], keep="last")


# MRI

df_mri = \
    df_um_cln_mut[df_um_cln_mut['subject_id'].isin(ids_mri)].\
    drop_duplicates(subset=['subject_id'], keep="last")


# PET

df_pet = df_um_cln_mut[df_um_cln_mut['subject_id'].isin(ids_pet)].\
    drop_duplicates(subset=['subject_id'], keep="last")



################################################################################
# Answer Questions
################################################################################

# Completed Baseline

df_um_cln_mut_bl = df_um_cln_mut.drop_duplicates(subset=['subject_id'], keep="first")
codes_mci = pd.Series([1, 2, 31, 34])
codes_ad = pd.Series([3, 4])

# NC 65-85; NL = 26
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['age'] >= 65) &
                     (df_um_cln_mut_bl['age'] <= 85) &
                     (df_um_cln_mut_bl['uds_dx'] == 26)].index)

# MCI; MCI = 1,2,31,34
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['uds_dx'].isin(codes_mci))].index)

# 85+
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['age'] > 85)].index)

# AD; AD = 3,4
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['uds_dx'].isin(codes_ad))].index)

# AA MCI; Black = 2
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['race_value'] == 2) &
                     (df_um_cln_mut_bl['uds_dx'].isin(codes_mci))].index)

# AA NC
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['race_value'] == 2) &
                     (df_um_cln_mut_bl['uds_dx'] == 26)].index)

# Spa MCI; Hispanic = 4
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['race_value'] == 4) &
                     (df_um_cln_mut_bl['uds_dx'].isin(codes_mci))].index)

# Spa NC
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['race_value'] == 4) &
                     (df_um_cln_mut_bl['uds_dx'] == 26)].index)

# Spa AD
len(df_um_cln_mut_bl[(df_um_cln_mut_bl['race_value'] == 4) &
                     (df_um_cln_mut_bl['uds_dx'].isin(codes_ad))].index)

# Biomarkers Collected - Amyloid PET

# NC 65-85; NL = 26
len(df_pet[(df_pet['age'] >= 65) &
           (df_pet['age'] <= 85) &
           (df_pet['uds_dx'] == 26)].index)

# MCI; MCI = 1,2,31,34
len(df_pet[(df_pet['uds_dx']).isin(codes_mci)].index)

# 85+
len(df_pet[(df_pet['age'] > 85)].index)

# AD; AD = 3,4
len(df_pet[(df_pet['uds_dx'].isin(codes_ad))].index)

# AA MCI; Black = 2
len(df_pet[(df_pet['race_value'] == 2) &
           (df_pet['uds_dx'].isin(codes_mci))].index)

# AA NC
len(df_pet[(df_pet['race_value'] == 2) &
           (df_pet['uds_dx'].isin(codes_ad))].index)

# Spa MCI; Hispanic = 4
len(df_pet[(df_pet['race_value'] == 4) &
           (df_pet['uds_dx'].isin(codes_mci))].index)

# Spa NC
len(df_pet[(df_pet['race_value'] == 4) &
           (df_pet['uds_dx'] == 26)].index)

# Spa AD
len(df_pet[(df_pet['race_value'] == 4) &
           (df_pet['uds_dx'].isin(codes_ad))].index)

# Biomarkers Collected - Structural MRI

# NC 65-85; NL = 26
len(df_mri[(df_mri['age'] >= 65) &
           (df_mri['age'] <= 85) &
           (df_mri['uds_dx'] == 26)].index)

# MCI; MCI = 1,2,31,34
len(df_mri[(df_mri['uds_dx']).isin(codes_mci)].index)

# 85+
len(df_mri[(df_mri['age'] > 85)].index)

# AD; AD = 3,4
len(df_mri[(df_mri['uds_dx'].isin(codes_ad))].index)

# AA MCI; Black = 2
len(df_mri[(df_mri['race_value'] == 2) &
           (df_mri['uds_dx'].isin(codes_mci))].index)

# AA NC
len(df_mri[(df_mri['race_value'] == 2) &
           (df_mri['uds_dx'].isin(codes_ad))].index)

# Spa MCI; Hispanic = 4
len(df_mri[(df_mri['race_value'] == 4) &
           (df_mri['uds_dx'].isin(codes_mci))].index)

# Spa NC
len(df_mri[(df_mri['race_value'] == 4) &
           (df_mri['uds_dx'] == 26)].index)

# Spa AD
len(df_mri[(df_mri['race_value'] == 4) &
           (df_mri['uds_dx'].isin(codes_ad))].index)

# Biomarkers Collected - APOE

# NC 65-85; NL = 26
len(df_apoe[(df_apoe['age'] >= 65) &
            (df_apoe['age'] <= 85) &
            (df_apoe['uds_dx'] == 26)].index)

# MCI; MCI = 1,2,31,34
len(df_apoe[(df_apoe['uds_dx']).isin(codes_mci)].index)

# 85+
len(df_apoe[(df_apoe['age'] > 85)].index)

# AD; AD = 3,4
len(df_apoe[(df_apoe['uds_dx'].isin(codes_ad))].index)

# AA MCI; Black = 2
len(df_apoe[(df_apoe['race_value'] == 2) &
            (df_apoe['uds_dx'].isin(codes_mci))].index)

# AA NC
len(df_apoe[(df_apoe['race_value'] == 2) &
            (df_apoe['uds_dx'].isin(codes_ad))].index)

# Spa MCI; Hispanic = 4
len(df_apoe[(df_apoe['race_value'] == 4) &
            (df_apoe['uds_dx'].isin(codes_mci))].index)

# Spa NC
len(df_apoe[(df_apoe['race_value'] == 4) &
            (df_apoe['uds_dx'] == 26)].index)

# Spa AD
len(df_apoe[(df_apoe['race_value'] == 4) &
            (df_apoe['uds_dx'].isin(codes_ad))].index)
