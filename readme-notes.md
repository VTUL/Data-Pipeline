    # install pip
    pip install virtualenvwrapper
    # or whatever path your os uses
    source /usr/local/bin/virtualenvwrapper.sh
    # this command uses ssh keys on per project
    # basis see gitlab documentation
    git clone \
       git@code.vt.edu:dataservices/data-pipeline.git \
       [dest]   \
       --config core.sshCommand="ssh -i
            ~/.ssh/<private-key>" 
    cd 
    mkvirtualenv en-chant
    pip install -r requirements.txt
    #setup labrynth from template for local
    cp labtemp.py labrynth.py
    vi labrynth.py
    # set up environment variables from labtemp
    # in lambda function>configuration>environment
    # variables add a DP_ENV[test/dev/live] variable
    # as well
    # deploy
    ./package.sh
    # upload deploymentpackage.zip to lamda function
    # manual creation deploymentpackage.zip
    cd packages/ #where ever in virtualenv
    zip -r9 project-folder/deploymentpackage.zip *
    cd project-folder/
    zip -gr deploymentpackage.zip *

    # install awscli
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
