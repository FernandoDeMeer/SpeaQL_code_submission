import json
from json import JSONDecodeError
from pathlib import Path


GROUND_TRUTH_PATH = Path("data") / "queries" / "ground_truth.jsonl"
APP_PATH = Path("data") / "experiment_results" / "results_20260126_133426.jsonl"


def load_app_results(path: Path) -> list:
    """Load concatenated JSON objects from the experiment results file.

    The results file is not a single JSON array; it contains multiple standalone
    JSON documents placed back-to-back. We stream-decode them with
    ``JSONDecoder.raw_decode`` to reconstruct the list of objects.
    """

    decoder = json.JSONDecoder()
    with open(path, "r") as f:
        content = f.read()

    entries = []
    idx = 0
    length = len(content)

    while idx < length:
        # Skip whitespace between JSON documents
        while idx < length and content[idx].isspace():
            idx += 1

        if idx >= length:
            break

        try:
            obj, end = decoder.raw_decode(content, idx)
        except JSONDecodeError as exc:
            raise ValueError(f"Failed to decode results file at position {idx}: {exc}") from exc

        entries.append(obj)
        idx = end

    return entries

def count_proportion_valid_nodes(call_nr: int, app_data: list) -> tuple:
    output = extract_output_fields_app(app_data)
    count_validated = 0
    count_non_matching = 0

    counter = 1
    for response in output: 
        match = next(
        (r for r in response if r["call_number"] == call_nr),
        None
        )

        if match:
            count_validated += len(match.get("validated_nodes", []))
            count_non_matching += len(match.get("non_matching_nodes", []))
    total = count_validated + count_non_matching
    proportion_validated = count_validated / total if total > 0 else 0
    return proportion_validated, 1 - proportion_validated


def extract_data() -> list:
    with open(GROUND_TRUTH_PATH, 'r') as f:
        ground_truth_data = json.load(f)
    try:
        with open(APP_PATH, 'r') as f:
            app_data = json.load(f)
    except JSONDecodeError:
        app_data = load_app_results(APP_PATH)
    return ground_truth_data, app_data

# return ground truth values only
def extract_output_fields_database(data: list) -> list:
    return [item["ground_truth"] for item in data]

def extract_output_fields_app(data: list) -> list:
    return [item["responses"] for item in data]
    

# evaluate proportion of validated/non-validated nodes
def evaluate_validated_nodes_proportions(data: list) -> tuple:
    _, app_data = extract_data()
    count_validated = 0
    count_non_matching = 0
    for item in app_data:
        if item['validated_nodes']:
            count_validated += len(item['validated_nodes'])
        if item['non_matching_nodes']:
            count_non_matching += len(item['non_matching_nodes'])
    proportion_validated = count_validated/(count_validated + count_non_matching)
    return proportion_validated, 1-proportion_validated


def get_call(responses:list, n:int) -> list:
    return next((r for r in responses if r["call_number"] == n), None)

def get_dict_by_hash(data, target_hash):
    print('target hash:', target_hash)
    for elem in data:
        if elem[0][1] == target_hash:
            return elem[1]
    return None

# compute recall with respect to ground_truth. Consider only fields and values present in ground_truth also present in app 
def evaluate_recall(ground_truth_data:list, app_data:list, call_nr:int) -> float:
    # extract all output rows
    gt_data = extract_output_fields_database(ground_truth_data)
    app_data = extract_output_fields_app(app_data)
    # get list of responses of specific call
    app_call_data = [r for responses in app_data for r in responses if r["call_number"] == call_nr]

    # iterate over each query
    TP = 0
    FN = 0
    query_recalls = []
    for app_rows, gt_rows in zip(app_call_data, gt_data):
        TP = 0
        FN = 0
        if not gt_rows:
            continue
        else:
            for gt_row in gt_rows:
                gt_dict = gt_row[1]
                valid_nodes = app_rows['validated_nodes']
                validated_dicts = [elem[1] for elem in valid_nodes]
                non_matching_nodes = app_rows['non_matching_nodes']
                non_matching_dicts = [elem[1] for elem in non_matching_nodes]
                for field, value in gt_dict.items():
                    if value == 'null' or value is None:
                        continue # skip null values
                    found = False
                    for d in validated_dicts:
                        if field in d and d[field] == value:
                            TP += 1
                            found = True
                            break
                    for d in non_matching_dicts:
                        # Look for true positive in matching attributes
                        if field in d :
                            if type(d[field]) != dict:
                                if d[field] == value:
                                    TP += 1
                                    found = True
                                    break
                            elif type(d[field]) == dict and 'value_mismatch' in d[field].values():
                                # Handle substring matches in non-matching attributes as well
                                if type(d[field]['llm_value']) == str and type(value) == str:
                                    if d[field]['llm_value'] in value:
                                        TP += 1
                                        found = True
                                        break

                    if not found:
                        FN += 1
            # Calculate recall for this query
            query_recall = TP / (TP + FN) if (TP + FN) > 0 else 0
            query_recalls.append(query_recall)
    
    # Return average recall across all queries
    return sum(query_recalls) / len(query_recalls) if query_recalls else 0


if __name__=='__main__':
    ground_truth_data, app_data = extract_data()
    nr_calls = 3
    total_recall = 0
    total_proportion_validated_nodes = 0
    total_proportion_non_matching_nodes = 0

    for i in range(1, nr_calls + 1):
        total_recall += evaluate_recall(ground_truth_data, app_data, i)
        valid, non_valid = count_proportion_valid_nodes(i, app_data)
        total_proportion_validated_nodes += valid
        total_proportion_non_matching_nodes += non_valid
    print(f'Final averaged results:\n Avg recall: {total_recall/3}, avg proportion validated nodes: {total_proportion_validated_nodes/3}, avg proportion non-matching nodes: {total_proportion_non_matching_nodes/3}')

