# cis9440

Install requirements. I suggest using a virtual environment to avoid breaking your default Python install.
```
pip install -r requirements.txt
```

Set up your Oracle autonomous db account and install the Oracle instant client basic. You can follow instructions at the link below.
https://www.oracle.com/database/technologies/appdev/python/quickstartpython.html

Need to add the following environment variables via your system shell.

```
export oracle_user=your_oracle_autonomous_db_username
export oracle_password=your_oracle_autonomous_db_password
```

You will also have to set up your Kaggle credentials. 
The API needs a configuration to use. 
The simplest one is perhaps to write a kaggle.json file under the ".kaggle" folder in your home directory.
More information available at the link below.
https://www.kaggle.com/code/donkeys/kaggle-python-api
