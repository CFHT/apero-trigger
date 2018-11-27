bin_dir = '/data/spirou/trigger/'
drs_root = '/data/spirou/spirou-drs/INTROOT'
sessiondir = '/data/sessions/spirou/'

def set_drs_config_subdir(subdirectory):
    import os
    os.environ['DRS_UCONFIG'] = os.path.join(bin_dir, 'config', subdirectory)
