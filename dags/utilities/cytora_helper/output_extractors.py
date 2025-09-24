import re
from collections import defaultdict

from airflow.exceptions import AirflowException
from helpers.cytora_mappings import CYTORA_OUTPUT_FIELD_MAP_MAIN

from helpers.utils import get_field_value


def extract_main_outputs(output: dict):
    def get_output_value_from_key(key):
        return get_field_value(output, key)

    extracted_outputs = {}

    try:
        for output_key, (
            input_key,
            transform_fn,
        ) in CYTORA_OUTPUT_FIELD_MAP_MAIN.items():
            extracted_outputs[output_key] = transform_fn(
                get_output_value_from_key(input_key)
            )
        extracted_outputs["job_id"] = output["job_id"]
    except Exception as e:
        raise AirflowException(f"Failed to extract outputs: {e}")

    return extracted_outputs


def extract_sov_outputs(output: dict):
    sov_fields = output.get("fields", {}) or {}
    sov_rows = extract_sov_from_flat_fields(sov_fields)

    return sov_rows


def extract_sov_from_flat_fields(fields: dict):
    row_map = defaultdict(dict)
    pattern = re.compile(r"^property_list\.property_details\[(\d+)\]\.(.+)$")
    for key, field_obj in fields.items():
        m = pattern.match(key)
        if not m:
            continue
        idx = int(m.group(1))
        col = m.group(2)
        if isinstance(field_obj, dict):
            row_map[idx][col] = field_obj.get("value")
        else:
            row_map[idx][col] = field_obj
    return [row_map[i] for i in sorted(row_map.keys())]


def extract_renewal_outputs(output: dict):
    renewal_terms = get_field_value(output, "renewal_terms_comparison")
    extracted_renewal_output = {"renewal_terms_comparison": renewal_terms}
    return extracted_renewal_output
