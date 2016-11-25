# distributed_backup.py
Copy and compress a large directory for backing up the data. Because the  
directory is very large, distribute the task of compressing  and copying  
the data over distinct periods of time or across a computing cluster. Ensure  
some kind of error tolerance so that a flaw in one of the files in the large  
directory will not render the entire copy useless.


## REQUIREMENTS

* A Linux-like operating system
* Python 3.0 or newer
* Bash
* tar
* md5sum
* gzip


## HOW TO USE

### 1. Create A Compressed Copy Of A Directory

First, prepare the compression scripts by running

> distributed_backup.py --source a_dir --destination copy_of_a_dir --backup --no-interactive

which creates a new directory called 'copy_of_a_dir'. Then, execute the  
compression scripts whichever way suits you best, e.g.:

> for SCRIPT in copy_of_a_dir/.distributed_backup_jobs/todo/*.sh; do bash $SCRIPT; done  

Executing the scripts will copy and compress the data into 'copy_of_a_dir' and  
create checksums for the compressed files. When you are executing the scripts,  
you can run 

> distributed_backup.py --source a_dir --destination copy_of_a_dir  --check-backup-todo

which will move all the scripts whose results were found in their destination  
from  'copy_of_a_dir/.distributed_backup_jobs/todo'  
into  'copy_of_a_dir/.distributed_backup_jobs/todo'  

Finally, verify that the directory 'copy_of_a_dir' and its contents are  
present and intact:

> distributed_backup.py --source a_dir --destination copy_of_a_dir --verify-backup


### 2. Restore Into A New Decompressed Directory

Make the directory structure and the job scripts that are needed to  
decompress 'copy_of_a_dir' into 'a_dir_restored'

> distributed_backup.py --source copy_of_a_dir --destination a_dir_restored --restore --no-interactive

which creates a new directory called 'a_dir_restored' and the entire  
directory structure which was originally contained within 'a_dir'.  
Then, execute the decompression scripts, e.g.:

> for SCRIPT in a_dir_restored/.distributed_backup_jobs/todo/*.sh; do bash $SCRIPT; done  

Executing the script will compress the data within 'copy_of_a_dir' and create  
MD5 checksums for the compressed files. Running

> distributed_backup.py --source copy_of_a_dir --destination a_dir_restored  --check-restore-todo

will again move then successfully executed scripts from 'todo' into 'done'.  
Finally, verify that the directory  'copy_of_a_dir' and its contents are  
present and intact:

> distributed_backup.py --source copy_of_a_dir --destination a_dir_restored  --check-restore-todo


## DETAILS

### Running --backup

> distributed_backup.py --source a_dir --destination copy_of_a_dir --backup --no-interactive

Creates a new directory called 'copy_of_a_dir' which has the following structure:

copy_of_a_dir/  
├── catalog.txt  
├── catalog.txt.md5  
├── .distributed_backup_jobs/  
│   ├── done/  
│   └── todo/  
│       ├── copy_of_a_dir__some_subdirectory.sh  
│       └── copy_of_a_dir__other_subdirectory.sh  
└── files/  
    ├── copy_of_a_dir__some_subdirectory.loc  
    ├── copy_of_a_dir__some_subdirectory.loc.md5  
    ├── copy_of_a_dir__other_subdirectory.loc  
    └── copy_of_a_dir__other_subdirectory.loc.md5  


catalog.txt: a list of all the files and folders that were found in 'a_dir'  

.distributed_backup_jobs: a folder containing the shell scripts for compression or decompression  

files: a folder containing the actual data from 'a_dir'  

For each subdirectory within 'a_dir', a .loc file within files contains a list  
of the files and folders which were contained within that folder. 

### Executing The Compression Scripts

After using --backup option, the shell scripts within  
.distributed_backup_jobs/todo  
contain, for every subdirectory of 'a_dir', the necessary instructions to:  
1. compress the the files contained within that subdirectory using tar and gzip
2. move the compressed tar.gz file into the 'files' directory
3. create an MD5 checksum for the compressed file

So after executing 'copy_of_a_dir__some_subdirectory.sh' and  
'copy_of_a_dir__some_subdirectory.sh' in the example would change the structure  
of 'copy_of_a_dir' into  

copy_of_a_dir/  
├── catalog.txt  
├── catalog.txt.md5  
├── .distributed_backup_jobs/  
│   └── done/  
│       ├── copy_of_a_dir__some_subdirectory.sh  
│       └── copy_of_a_dir__other_subdirectory.sh  
│   └── todo/  
└── files/  
    ├── copy_of_a_dir__some_subdirectory.loc  
    ├── copy_of_a_dir__some_subdirectory.loc.md5  
    ├── copy_of_a_dir__some_subdirectory.tar.gz  
    ├── copy_of_a_dir__some_subdirectory.tar.gz.md5  
    ├── copy_of_a_dir__other_subdirectory.loc  
    ├── copy_of_a_dir__other_subdirectory.loc.md5  
    ├── copy_of_a_dir__other_subdirectory.tar.gz  
    └── copy_of_a_dir__other_subdirectory.tar.gz.md5  

### Executing The Decompression Scripts

After using --restore option, the shell scripts within  
.distributed_backup_jobs/todo  
contain, for every subdirectory of 'a_dir', the necessary instructions to  
decompress the file into the correct location within 'a_dir_restored'.