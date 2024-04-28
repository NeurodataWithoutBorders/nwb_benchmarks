from typing import Any, Dict, Union


class BaseBenchmark:
    """
    The ASV convention is to specify parametrized fields as a pair of `param_names` and `params`.

    This was deemed less readable than a single dictionary of parameter cases.

    So inheriting from a `BaseBenchmark` allows us to specify parameter cases as the form...

    parameter_cases = dict(
        ParameterCase1=dict(
            parameter1=value1,
            parameter2=value2,
            ...
        ParameterCase2=dict(
            parameter1=value3,
            parameter2=value4,
            ...
        ),
        ...
    )

    ...and this will be unpacked into the expected form by ASV.
    """

    parameter_cases: Union[Dict[str, Dict[str, Any]], None] = None

    def __init__(self):
        # Unpack parameter cases dictionary into ASV expected form
        if self.parameter_cases is not None:
            self.param_names = list(next(iter(self.parameter_cases.values())).keys())
            self.params = (
                [
                    [parameter_case[param_name] for parameter_case in self.parameter_cases.values()]
                    for param_name in self.param_names
                ]
                if len(self.param_names) > 1
                else [
                    parameter
                    for parameter_case in self.parameter_cases.values()
                    for parameter in parameter_case.values()
                ]
            )
