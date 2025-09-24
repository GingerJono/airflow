from helpers.cytora_helper import CytoraHook
from helpers.cytora_mappings import CYTORA_OUTPUT_FIELD_MAP_MAIN


def get_missing_mapping_keys(cytora_instance: CytoraHook) -> set[str]:
    """
    Return the set of mapping keys that are not present in the required
    output fields of the Cytora schema.

    Mapping keys are taken from the first element of each tuple in
    CYTORA_OUTPUT_FIELD_MAP_MAIN.values().
    """

    mapping_keys = [value[0] for value in CYTORA_OUTPUT_FIELD_MAP_MAIN.values()]
    required_output_fields = cytora_instance.get_schema_required_output_fields()
    return set(mapping_keys) - set(required_output_fields)
