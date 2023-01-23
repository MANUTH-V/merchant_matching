import pandas as pd
from rapidfuzz import process, fuzz
import tqdm
import rapidfuzz
import numpy as np
import time

def Jaro_rapid_fuzz(input: list, lookup):
        levenshtein_ls = []

        for x in tqdm.tqdm(input):
            
            levenshtein = process.extract(x, lookup, scorer=rapidfuzz.distance.Jaro.distance, limit=1)
            levenshtein_ls.append(levenshtein)
            

        return levenshtein_ls

def out2pd(target_pd, test):
    aba_merchat_name_matched = [i[0][0] for i in test]
    aba_merchat_score_matched = [i[0][1] for i in test]
    aba_merchat_id_matched = [i[0][2] for i in test]

    khqr_to_aba_matched = pd.DataFrame({
        'aba_merchat_name_matched' : aba_merchat_name_matched,
        'aba_merchat_score_matched' : aba_merchat_score_matched,
        'aba_merchat_id_matched' : aba_merchat_id_matched
    })

    result = target_pd.merge(khqr_to_aba_matched, left_index=True, right_index=True)

    return result

def start(qr_path:str, pw_path: str, result_path: str):

    khqr = pd.read_csv(qr_path)

    pw = pd.read_csv(pw_path)

    pw['merchant_name'] = pw['merchant_name'].astype(str).str.replace(u'\u200b', "")

    khqr['merchant_name'] = khqr['merchant_name'].astype(str).str.replace(u'\u200b', "")

    pw['merchant_name'] = pw['merchant_name'].str.strip()

    khqr['merchant_name'] = khqr['merchant_name'].str.strip()

    khqr['merchant_name'] = khqr['merchant_name'].replace(r'\s+', ' ', regex=True)

    pw['merchant_name'] = pw['merchant_name'].replace(r'\s+', ' ', regex=True)

    pw_dict = dict(list(zip(pw["merchant_id"], pw["merchant_name"])))

    target = khqr['merchant_name'].to_list()

    jaro = Jaro_rapid_fuzz(target, pw_dict)

    jaro_final = out2pd(pd.DataFrame(khqr['merchant_name']), jaro)

    jaro_final.to_csv(result_path, index=False)