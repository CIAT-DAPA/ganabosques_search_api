pipeline {
    agent any

    environment {
        server_name   = credentials('ganabosques_name')
        server_host   = credentials('ganabosques_host')
        ssh_key       = credentials('ganabosques')
        ssh_key_user  = credentials('ganabosques_user')
    }

    stages {

        stage('Connection to AWS server') {
            steps {
                script {
                    remote = [
                        allowAnyHosts: true,
                        identityFile: ssh_key,
                        user: ssh_key_user,
                        name: server_name,
                        host: server_host
                    ]
                }
            }
        }

        stage('Deploy Search API') {
            steps {
                script {
                    sshCommand remote: remote, command: '''
                        set -e

                        CONDA_ENV_PATH="/home/ganabosques/.miniforge3/envs/api/bin"
                        APP_DIR="/opt/ganabosques/api/ganabosques_search_api"
                        APP_PORT=5001

                        echo "Matando proceso en puerto $APP_PORT..."
                        fuser -k $APP_PORT/tcp || true

                        echo "Entrando a la carpeta del proyecto..."
                        cd $APP_DIR

                        echo "Haciendo pull del código..."
                        git pull origin main

                        echo "Instalando dependencias con conda env admin..."
                        $CONDA_ENV_PATH/pip install -r ./src/requirements.txt

                        echo "Levantando servicio Uvicorn..."
                        nohup $CONDA_ENV_PATH/uvicorn src.main:app --host 0.0.0.0 --port $APP_PORT > app.log 2>&1 &

                        echo "Deploy finalizado correctamente."
                    '''
                }
            }
        }
    }

    post {
        failure {
            echo '❌ Deploy failed'
        }

        success {
            echo '✅ Everything went very well!!'
        }
    }
}
