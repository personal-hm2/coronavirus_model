# This code should run as a separate .py file in a command line.
# Running it in Notebook is *very* slow

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import pickle

data_dir = "/mnt/f/Users/Hiroshi/Desktop/DoCoMo/"
# data_dir = "f:/Users/Hiroshi/Desktop/DoCoMo/"
cache_dir = data_dir + "cache/"
debug = False

def read_cluster(cluster_fn):
    pred = None
    if os.path.exists(cluster_fn):
        with open(cluster_fn, 'rb') as f:
            pred = pickle.load(f)
        print('Using saved cluster', cluster_fn)
    return pred

cluster = read_cluster(cache_dir + 'cluster.pickle')     

patterns = {"00000":["area"], 
            "00000_c": ["area"],
           "00001_c":["area", "age", "gender"],
           "00002_c":["area", "residence"],
           "00003":["residence"]}

tables = {}

def read_cache(pattern):
    fn = cache_dir + pattern + ".pickle"
    if os.path.exists(fn):
        with open(fn, 'rb') as f:
            print("Reading cache", fn)
            return pickle.load(f)
    else:
        return None

def read_hourly_data(pattern, d, h):
    d_str = d.strftime("%Y%m%d")
    d_h = d + pd.Timedelta('{} hours'.format(h))
    fn = data_dir + "s3/realtime/{}/clipped_mesh_pop_{}{:02d}00_{}.csv.zip" \
      .format(d_str, d_str, h, pattern[0:5])
    print("Reading {}...".format(fn))
    if os.path.exists(fn):
        with open(fn, 'rb') as f:
            hourly = pd.read_csv(fn, usecols=patterns[pattern] + ["population"], dtype={"area": str})
            hourly = hourly.set_index(patterns[pattern])
            hourly.rename(columns={"population": d_h}, inplace=True)
    else:
        hourly = pd.DataFrame(columns=patterns[pattern] + ["population"])
        hourly.set_index(patterns[pattern], inplace=True)
        hourly.rename(columns={"population": d_h}, inplace=True)
    return hourly

def add_hour(pattern, t_1, t, m):
    if debug:
        print("t-1")
        display(t_1)
        print("t")
        display(t)
    # 1. Calculate Norm
    df = t_1.merge(t, how='outer', left_index=True, right_index=True)
    df.fillna(0, inplace=True)
    if debug:
        print("After merge")
        display(df)
    norm = df.diff(axis=1).abs()
    norm.reset_index(inplace=True)
    norm.set_index("area", inplace=True)
    if debug:
        print("Norm")
        display(norm)
    # 2. Join category
    df = norm.merge(cluster, how='right', left_index=True, right_index=True)
    df.fillna(0, inplace=True)
    df = df.astype('int')
    df.rename(columns={0:"cluster"}, inplace=True)

    if debug:
        print("With category")
        display(df)
    # 3. Group them
    df = df.groupby(["cluster"] + patterns[pattern][1:]).mean()
    if debug:
        print("Grouped by")
        display(df)
        print("m")
        display(m)
    # 4. Merge it to m
    df = m.merge(df.iloc[:,1], how='outer', left_index=True, right_index=True)
    df.fillna(0, inplace=True)
    if debug:
        print("mobility_by_category")
        display(df)
    return df

def update_cache(pattern, end_date=None):
    mobility_by_cat = read_cache(pattern)
    if mobility_by_cat is None:        
        start_date = pd.to_datetime("2020-01-01")
        # mobility_by_cat = pd.DataFrame(columns=patterns[pattern] + ["population"])
        # mobility_by_cat = mobility_by_cat .set_index(patterns[pattern])
        # mobility_by_cat.rename(columns={"population": start_date}, inplace=True)
        mobility_by_cat = pd.DataFrame(columns=["cluster"] + patterns[pattern][1:])
        mobility_by_cat.set_index(["cluster"] + patterns[pattern][1:], inplace=True)
        # print(mobility_by_cat)
    else:
        start_date = pd.to_datetime(mobility_by_cat.columns[-1]) + pd.Timedelta('1 days')
    if end_date is None:
        end_date = pd.Timestamp.today() - pd.Timedelta('1 days')
    dates_to_add = pd.date_range(start=start_date.date(), end=end_date)
    if len(dates_to_add) == 0:
        print("Cache is up-to-date.")
        return
    # print(dates_to_add)
    hourly_last = read_hourly_data(pattern, dates_to_add[0] - pd.Timedelta('1 day'), 23) # 前日の23時
    for d in dates_to_add:
        for h in range(24):
            hourly = read_hourly_data(pattern, d, h)
            mobility_by_cat = add_hour(pattern, hourly_last, hourly, mobility_by_cat)
            hourly_last = hourly
            if debug and h > 1:
                return
               
    with open(cache_dir + pattern + ".pickle", 'wb') as f:
        pickle.dump(mobility_by_cat, f)

def read_table(pattern):
    update_cache(pattern)
    tables[pattern] = read_cache(pattern)

# update_cache("00001_c", end_date=pd.to_datetime("2020-01-03"))
update_cache("00000_c")
update_cache("00001_c")
update_cache("00002_c")

# update_cache("00000", end_date=pd.to_datetime("2020-02-29"))
# t = read_hourly_data("00000", pd.to_datetime("2020-01-01"), 0)
# pred.merge(t, left_index=True, right_index=True, how='left').groupby(0).sum()
print('Done')