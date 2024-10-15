import lsst.daf.butler as daf_butler
import concurrent.futures
import json
import logging
from itertools import islice

repo_path = '/repo/embargo'
collection = 'LSSTComCam/raw/all'


# Function to create a Butler instance
def create_butler(repo_path):
    return daf_butler.Butler(repo_path)


# Function to convert dataId to a JSON-serializable format using toSimple()
def serialize_data_id(data_id):
    # Use toSimple() to convert the _FullTupleDataCoordinate into a dictionary
    return data_id.to_json()


# Function to write data to file immediately
def write_to_file(data_ref, path_file):
    # Serialize the data_ref.dataId using toSimple() to ensure it is JSON-serializable
    serializable_data_id = serialize_data_id(data_ref.dataId)
    # print(f"Writing data to file: {serializable_data_id}")  # Debugging print
    with open(path_file, 'a') as fout:
        json.dump(serializable_data_id, fout)  # Dump the serialized data_id to file
        fout.write('\n')  # Ensure newline after each entry for readability
        fout.flush()  # Flush to ensure data is written immediately


# Function to process each data reference with worker-specific Butler access
def process_with_butler(data_ref, path_file='dark_visitIDs.json'):
    try:
        butler = create_butler(repo_path)  # Each process will get its own Butler instance
        header = butler.get('raw.metadata', collections=collection, dataId=data_ref.dataId)
        img_type = header.get('IMGTYPE')
        if img_type == "DARK":
            write_to_file(data_ref, path_file=path_file)
        return data_ref, img_type
    except Exception as e:
        print(f"Error retrieving metadata for {data_ref.dataId}: {e}")
        return data_ref, None


# Query for raw datasets
def query_data_refs():
    butler = daf_butler.Butler(repo_path)
    data_refs = list(butler.registry.queryDatasets(
        datasetType="raw", collections=collection, where="instrument='LSSTComCam'"))
    return data_refs


# Batch the dataset to reduce memory usage
def batched(iterable, n):
    """Batch data into chunks of size n."""
    iterator = iter(iterable)
    for first in iterator:
        yield list(islice(iterator, n - 1))


# Main function for parallel processing with memory optimizations and progress tracking
def get_all_dark_visitid(repo_path, batch_size=100, max_workers=2):
    # Lower the logging level for LSST translators to suppress warnings
    logging.getLogger('lsst.obs.lsst.translators.comCam').setLevel(logging.ERROR)
    data_refs = query_data_refs()
    total = len(data_refs)
    completed = 0

    # Clear file content at the beginning
    open('dark_visitIDs.json', 'w').close()

    # Process data in batches
    for batch in batched(data_refs, batch_size):
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_with_butler, data_ref): data_ref for data_ref in batch}

            for future in concurrent.futures.as_completed(futures):
                data_ref, img_type = future.result()

                # Update and print progress
                completed += 1
                print(f"\rProgress: {completed}/{total} completed ({(completed / total) * 100:.2f}%)", end="")


def read_json_file(file_path):
    data_list = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                # Load each line as a dictionary and append to the list
                data = json.loads(json.loads(line.strip()))
                data_list.append(data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    return data_list