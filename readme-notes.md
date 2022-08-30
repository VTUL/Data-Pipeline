    git pull
    mkvirtualenv ch
    pip install -r requirements.txt
    #setup labrynth from template
    cp labrynth_template.py labrynth.py
    vi labrynth.py
    cd packages/ #where ever in virtualenv
    zip -r9 project-folder/deploymentpackage.zip *
    cd project-folder/
    zip -gr deploymentpackage.zip *
    # todo script this?
