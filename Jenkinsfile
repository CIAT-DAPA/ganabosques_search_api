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

        stage('Deploy FastAPI service') {
            steps {
                script {
                    sshCommand remote: remote, command: '''
                        echo "Matando proceso en puerto 5001..."
                        fuser -k 5001/tcp || true

                        echo "Entrando a la carpeta de la API..."
                        cd /opt/ganabosques/api/ganabosques_search_api

                        echo "Haciendo pull del código..."
                        git pull origin main

                        echo "Instalando dependencias en entorno conda..."
                        /home/ganabosques/.miniforge3/envs/api/bin/pip install -r src/requirements.txt

                        echo "Levantando servicio con uvicorn..."
                        cd src
                        nohup /home/ganabosques/.miniforge3/envs/api/bin/uvicorn main:app --host 0.0.0.0 --port 5001 > api.log 2>&1 &
                    '''
                }
            }
        }
    }

    post {
        failure {
            script {
                echo 'fail :c'
            }
        }

        success {
            script {
                echo 'everything went very well!!'
            }
        }
    }
}
