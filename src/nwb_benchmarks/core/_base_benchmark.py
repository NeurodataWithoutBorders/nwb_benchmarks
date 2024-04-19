from typing import Any, Dict, Union


class BaseBenchmark:
    parameter_cases: Union[Dict[str, Dict[str, Any]], None] = None

    def __init__(self):
        # Unpack parameter cases dictionary into ASV expected form
        if self.parameter_cases is not None:
            self.param_names = list(next(iter(self.parameter_cases.values())).keys())
            self.params = (
                [
                    [parameter_case[param_name] for param_name in self.param_names]
                    for parameter_case in self.parameter_cases.values()
                ]
                if len(self.param_names) > 1
                else [
                    parameter
                    for parameter_case in self.parameter_cases.values()
                    for parameter in parameter_case.values()
                ]
            )
        super().__init__()
