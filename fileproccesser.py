import glob
import os
import subprocess
import sys

class ParallelFileProcessManager:
    def __init__(self, file_pattern, scripts):
        self.file_process_lists = {}
        self.file_pattern = file_pattern
        self.scripts = scripts

    def run(self):
        self.process_new_files()
        self.cleanup_completed()

    def process_new_files(self):
        all_files = self.find_files()
        for file in all_files:
            if (file not in self.file_process_lists):
                self.process_file(file)

    def cleanup_completed(self):
        for file in list(self.file_process_lists.keys()):
            self.file_cleanup_check(file)

    def find_files(self):
        return glob.glob(self.file_pattern)

    def process_file(self, file):
        p = []
        for script in self.scripts:
            p.append(subprocess.Popen([script, file], stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        self.file_process_lists[file] = p

    def file_cleanup_check(self, file):
        processes = self.file_process_lists[file]
        processes[:] = [process for process in processes if not self.process_cleanup_check(process)]
        if not processes:
            del self.file_process_lists[file]
            os.remove(file)

    def process_cleanup_check(self, process):
        if process.poll() is not None:
            output = process.stdout.readline()
            while output:
                print(output.strip())
                output = process.stdout.readline()
            return True
        return False


def blocking_subprocess(script, args, interpreter = None, env = None):
    if interpreter is None:
        script_array = [script]
    else:
        script_array = [interpreter, script]
    p = subprocess.Popen(script_array + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    output = p.communicate()
    if output[0]:
        print(output[0].rstrip().decode('UTF-8'))
    if output[1]:
        sys.stderr.write(output[1].decode('UTF-8'))
