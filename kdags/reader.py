from kdags.assets.planification.cc.reader import read_raw_cc
from kdags.assets.reparation.component_status.reader import read_raw_component_status

from dataclasses import dataclass


@dataclass
class KVerseReader:
    class Planification:
        read_raw_cc = staticmethod(read_raw_cc)

    class Reparation:
        read_raw_component_status = staticmethod(read_raw_component_status)

    class Operation:
        pass

    class Maintenance:
        pass
        # self.components = ComponentsData()  # cc(), arrivals(), etc.
        # self.operations = OperationsData()  # events(), effective_grade(), etc.
        # self.personnel = PersonnelData()  # attendances(), etc.
