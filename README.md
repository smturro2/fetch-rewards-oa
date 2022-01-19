# Turro's Personal Site

[comment]: <> ([![Website http://TurroScott.web.illinois.edu ]&#40;https://img.shields.io/website-up-down-green-red/http/shields.io.svg&#41;]&#40;http://quantumtomo.web.illinois.edu/&#41;)

This website is to provide a more detailed view of my work than a standard resume:
[turroscott.web.illinois.edu](http://TurroScott.web.illinois.edu)

## Setup
This python application is built, tested, and hosted around python 3.8. 
For best results [install this version of python](https://www.python.org/downloads/). 

1. **Clone** the repo to your local computer


2. **Create a virtual environment** by typing the following in the command prompt. Make sure you are at the top most level of the repo.

         py -m venv src\venv
3. **Activate virtual environment**

        src\venv\Scripts\activate.bat

4. Install the **requirements**. 

         pip install -r src\requirements.txt
         
5. You can add the virtual environment to pycharm by going to File->Settings->Project->python interpreter. 
   Click the drop down for interpreter and click show all. Click the + button, and add a new existing interpreter using the path:
   _C:\path\to\local\repo\src\venv\Scripts\python.exe_
   

6. **Clone the Submodules**. This code uses the quantum-tomography python code as a submodule. You will need to update this when
you first clone this repo.

        git submodule update --init

---


* To run the app just run the **app.py** script


* To update the Quantum-Tomography code run the command:

        git submodule update --remote --merge

* To update the live site go into the cpanel terminal run:

        cd Flask_App_new/Flask_App_git_repo_new
        git status
        git pull

## Troubleshooting

- *'py' is not recognized*. 
  - Depending on how you downloaded python your PATH variable for python (the _py_ in this walkthrough) may be diffrent.
Other common names are _python_ or _python3_. Its whatever you type into the command prompt in order to start python. 
  Replace the _py_ with whatever your PATH variable for python is. Follow [this tutorial](https://www.educative.io/edpresso/how-to-add-python-to-path-variable-in-window) 
  if you can't figure out your python's PATH variable and you know for certain python is installed.

- *'pip' is not recognized*. 
  - pip is not defined as a PATH variable. Instead of just using *pip*
use *py -m pip*. It is rare but if you downloaded python in a weird way you may not have pip installed. You'll have to search for how to install pip on your computer. 
The process differs depending on if you're using PC/mac/ubuntu.
    