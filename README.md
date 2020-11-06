# apero-trigger
This project contains auxiliary functionality for the APERO pipeline.

## Overview:
#### 1. High-level reduction control tool
Note: the reduction portion of this tool has largely been superseded
by the `apero_processing.py` tool included with APERO.
#### 2. Creating packaged data products from reduced files
This includes the e.fits, s.fits, t.fits, v.fits, p.fits files.
#### 3. Hooks into observatory-specific interfaces
Examples include database read/write interfaces and custom error handling.
#### 4. Realtime data reduction

## Running:
Items 1 and 2 are available through the `offline_trigger.py`. Items 3 and 4 required `full_trigger.py`.

Running with the `-h` flag will provide full usage info for each script.

#### Creating products from reduced data
One possible usage scenario would be to take an existing night of data that has already been reduced by APERO
and create data products from the reduced files. The command to do this would be:
```
offline_trigger.py night [NIGHT] --steps products
```
Note that the trigger uses the data paths from the DRS. What this means is:
1. You need to have the apero environment active i.e. have the correct apero config loaded.
2. It expects the raw/preprocessed/reduced files to all be present in the locations APERO would look for/put them.

## Installation:
For `offline_trigger.py`, no dependencies are required beyond APERO and its dependencies.

On the other hand, to run the realtime reduction trigger, the dependencies in `requirements.txt` are needed:
```
pip install -r requirements.txt
```

Optionally, the trigger can use the [SPIRou-Polarimetry](https://github.com/edermartioli/spirou-polarimetry)
module to create p.fits polar products. This is currently supported through the
[CFHT fork](https://github.com/CFHT/spirou-polarimetry/tree/cfht) of the project.
Place the `spirou-polarimetry` directory alongside the `apero-drs` directory for the trigger to detect it.

## Development:
To contribute to the development of the trigger, installing pytest is useful be able to create and/or run tests.
The file `requirements_dev.txt` includes this as well including `requirements.txt`.
```
pip install -r requirements_dev.txt
```

The project is split up into three main packages:
1. `trigger` - everything needed to run `offline_trigger.py` (except for `logger.py`) 
2. `cfht` - various hooks into CFHT-specific interfaces
3. `realtime` - code for the realtime reduction system

Additional top level modules in the project are:
1. `logger` - used for logging across the entire project
2. `drsloader` - can be used to set a custom config prior to importing apero
3. `offline_trigger` and `full_trigger` - command line tools for accessing functionality of the trigger
4. `test` - pytest tests for certain components of the trigger
