import csv
import os
import datetime

from boxsdk import OAuth2, Client


def get_box_client():

    def list_folder(folder):
        for item in folder.get_items():
            print(item.name, item.id)

    auth = OAuth2(
        client_id='',
        client_secret='',
        access_token='',
    )
    client = Client(auth)
    user = client.user().get()
    print('The current user ID is {0}'.format(user.id))
    root = client.root_folder()
    list_folder(root)
    return client


def save_box_filenames_to_csv(box_client, box_folder_id, csv_fn):

    def write_csv(folder, fn="out.csv"):

        fn_list = []
        with open(fn, mode='w') as f:
            w = csv.writer(f, delimiter=',')
            w.writerow(['filename', 'id'])
            counter = 0
            for item in folder.get_items():
                counter += 1
                fn_list.append(item.name)
                print(item.name, item.id)
                w.writerow([item.name, item.id])
                if counter % 200 == 0:
                    f.flush()
        print("Done")
        return fn_list

    client = box_client
    f = client.folder(box_folder_id)

    return write_csv(f, fn=csv_fn)


def check_missings(model_start_year,
                   model_start_month,
                   model_start_day,
                   model_start_hour,
                   model_end_year,
                   model_end_month,
                   model_end_day,
                   model_end_hour,
                   box_csv,
                   out_csv=None):

    def expected_cosmo_outputs(model_start_dt):
        outputs = []
        model_end_dt = model_start_dt + datetime.timedelta(days=7, hours=6)
        model_timestep_td = datetime.timedelta(hours=1)
        model_start_str = model_start_dt.strftime("%Y%m%d%H")
        cur_timestep_dt = model_start_dt
        while cur_timestep_dt <= model_end_dt:
            cur_timestep_str = cur_timestep_dt.strftime("%Y%m%d%H")
            fn = "SOUTHNC_{}_{}.nc".format(model_start_str, cur_timestep_str)
            outputs.append(fn)
            cur_timestep_dt += model_timestep_td
        return outputs

    def check_missing_outputs(model_run_dt, files_dict):
        missing_counter = 0
        missing_list = []
        outputs = expected_cosmo_outputs(model_run_dt)
        print("Missing outputs for {}".format(model_run_dt))
        for fn in outputs:
            if fn not in files_dict:
                missing_counter += 1
                missing_list.append(fn)
                print(fn)
        print("-"*10 + "count: {}".format(missing_counter) + "-"*10)
        return missing_list

    def get_fn_id_dict(csv_fn):
        mydict = None
        with open(csv_fn, mode='r') as infile:
            reader = csv.reader(infile)
            mydict = {rows[0]: rows[1] for rows in reader}
        return mydict

    existing_fn_dict = get_fn_id_dict(box_csv)

    model_run_dt_start = datetime.datetime(year=model_start_year,
                                 month=model_start_month,
                                 day=model_start_day,
                                 hour=model_start_hour)

    model_run_dt_end = datetime.datetime(year=model_end_year,
                                 month=model_end_month,
                                 day=model_end_day,
                                 hour=model_end_hour)

    cur_model_run_dt = model_run_dt_start
    missing_list_total = []
    while cur_model_run_dt <= model_run_dt_end:
        missing_list_total += check_missing_outputs(cur_model_run_dt, existing_fn_dict)
        cur_model_run_dt += datetime.timedelta(hours=12)

    if out_csv is not None:
        with open(out_csv, mode='w') as f:
            w = csv.writer(f, delimiter=',')
            counter = 0
            for item in missing_list_total:
                counter += 1
                w.writerow([item])
                if counter % 200 == 0:
                    f.flush()

    return missing_list_total


def upload2box(box_client, folder_id, files):

    local_missings = []
    failed = []

    for f_source in files:
        if not os.path.isfile(f_source):
            local_missings.append(f_source)
            continue
        fn = os.path.basename(f_source)

        try:
            file_object = box_client.folder(folder_id).upload(f_source, fn)
        except Exception as ex:
            print(ex)
            failed.append(f_source)
    return failed, local_missings


if __name__ == "__main__":

    box_client = get_box_client()
    existings_csv_fn = "box_list.csv"
    box_folder_id = 0

    ## export existing filenames on Box to csv
    # save_box_filenames_to_csv(box_client, box_folder_id, existings_csv_fn)

    # check missings
    model_start_year = 2018
    model_start_month = 11
    model_start_day = 20
    model_start_hour = 12

    model_end_year = 2019
    model_end_month = 4
    model_end_day = 12
    model_end_hour = 0
    missing_csv = "missings_list.csv"

    missings=check_missings(model_start_year,
                    model_start_month,
                    model_start_day,
                    model_start_hour,
                    model_end_year,
                    model_end_month,
                    model_end_day,
                    model_end_hour,
                    existings_csv_fn,
                    out_csv=missing_csv)

    local_storage_base = "/home/hydro/Desktop"

    missings = [os.path.join(local_storage_base, fn) for fn in missings]

    failed, local_missings = upload2box(box_client, box_folder_id, missings)
    print(failed)
    print(local_missings)
