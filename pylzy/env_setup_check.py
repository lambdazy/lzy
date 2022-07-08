# import os
# import platform
# import re
# import subprocess
# from abc import ABC, abstractmethod
# from typing import List, Optional
#
# JAVA_VERSION_MIN = 17
#
#
# def is_mac() -> bool:
#     return platform.system() == "Darwin"
#
#
# class StageResult:
#     def __init__(self, result: bool, error_message: str = None):
#         self._error_message = error_message
#         self._result = result
#
#     def done(self) -> bool:
#         return self._result
#
#     def error_message(self) -> Optional[str]:
#         return (
#             f"\n\n########################################\n\n"
#             f"FAILED TO INSTALL LZY\n\nREASON: {self._error_message}\n\n"
#             f"########################################"
#         )
#
#
# class Stage(ABC):
#     @abstractmethod
#     def check(self) -> bool:
#         pass
#
#     @abstractmethod
#     def apply(self) -> StageResult:
#         pass
#
#     def run(self) -> StageResult:
#         if self.check():
#             return StageResult(True)
#         return self.apply()
#
#
# class JavaCheckStage(Stage):
#     _version_pattern = re.compile(r'"(\d+.\d+).*"')
#
#     def __init__(self):
#         super().__init__()
#         self._error = None
#         self._checked = False
#
#     def check(self) -> bool:
#         try:
#             out = str(
#                 subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
#             )
#
#             search = self._version_pattern.search(out)
#             if not search:
#                 self._error = "Wrong java version"
#                 return False
#
#             version = int(float(search.groups()[0]))
#             if version < JAVA_VERSION_MIN:
#                 self._error = "Java >= 17 is required"
#                 return False
#             return True
#         except (subprocess.CalledProcessError, FileNotFoundError):
#             self._error = "Java is required (>= 17)"
#             return False
#
#     def apply(self) -> StageResult:
#         if self._checked:
#             return StageResult(True)
#         # Do we really need to try to install Java here?
#         return StageResult(False, self._error)
#
#
# class FuseCheckStage(Stage):
#     def __init__(self):
#         super().__init__()
#         self._error = None
#
#     def check(self) -> bool:
#         if not is_mac():
#             # it is better to pass this stage if we are not sure
#             return True
#
#         if os.path.isfile("/usr/local/lib/libosxfuse.dylib"):
#             return True
#         else:
#             self._error = "macFUSE lib is required https://osxfuse.github.io"
#             return False
#
#     def apply(self) -> StageResult:
#         return StageResult(False, self._error)
#
#
# def check():
#     stages: List[Stage] = [JavaCheckStage(), FuseCheckStage()]
#     for stage in stages:
#         result = stage.run()
#         if not result.done():
#             raise ValueError(result.error_message())
