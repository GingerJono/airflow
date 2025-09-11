from helpers.utils import safe_parse_date, safe_stringify

CYTORA_OUTPUT_FIELD_MAP_MAIN = {
    "OutputInsuredName": ("insured_name", safe_stringify),
    "OutputInsuredDomicile": ("insured_domicile", safe_stringify),
    "OutputInsuredState": ("insured_state", safe_stringify),
    "OutputReinsuredName": ("reinsured_name", safe_stringify),
    "OutputReinsuredDomicile": ("reinsured_domicile", safe_stringify),
    "OutputReinsuredState": ("reinsured_state", safe_stringify),
    "OutputBrokerCompany": ("broker_company", safe_stringify),
    "OutputBrokerContact": ("broker_contact", safe_stringify),
    "OutputLineOfBusiness": ("line_of_business", safe_stringify),
    "OutputPolicyType": ("policy_type", safe_stringify),
    "OutputUnderwriter": ("underwriter", safe_stringify),
    "OutputInceptionDate": ("inception_date", safe_parse_date),
    "OutputExpiryDate": ("expiry_date", safe_parse_date),
    "OutputRiskLocation": ("risk_location", safe_stringify),
    "OutputAdditionalParties": ("additional_parties", safe_stringify),
    "OutputSubmissionSummary": ("submission_summary", safe_stringify),
    "OutputNewRenewal": ("new_vs_renewal", safe_stringify),
    "OutputRenewedFrom": ("renewed_from", safe_stringify),
}
