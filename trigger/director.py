import subprocess


def director_message(message: str, level: str = None):
    if level:
        message = level + ': ' + message
    command = '@say_ ' + message + '\n'
    subprocess.run(['nc', '-q', '0', 'spirou-session', '20140'], input=command, encoding='ascii')
