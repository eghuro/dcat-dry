pipeline {
	agent none
	stages {
		stage ('QA') {
			agent { label 'use' }
			steps {
				script {
					withPythonEnv('python3') {
					    sh 'python3 -m pip install --upgrade pip'
						sh 'pip install --use-feature=fast-deps --use-deprecated=legacy-resolver -r requirements.txt'
						sh 'pip check'
				    	sh 'pip install radon'
						sh 'radon raw --json tsa/ > raw_report.json'
						sh 'radon cc --json tsa/ > cc_report.json'
						sh 'radon mi --json tsa/ > mi_report.json'
						sh 'flake8 tsa || true'
					}
					def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
					withSonarQubeEnv('sonar') {
						sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY"
					}
				}
			}
		}
		
		stage('Build docker') {
			agent { label 'docker' }
			steps {
				script { 	 
					dockerImage = docker.build "eghuro/dcat-dry"
				}
			}
		}
		
		stage ('Push docker') {
			agent { label 'docker' }
			steps {
				script {
					GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
					docker.withRegistry('https://registry.hub.docker.com', '166025e7-79f5-41bf-825f-7d94c37af5cf') {
						dockerImage.push("${env.BUILD_NUMBER}")
						dockerImage.push("${GIT_COMMIT_HASH}")
						dockerImage.push("latest")
					}	 
				}
			}
		}
	}
	post {
        always {
            mattermostSend "Completed ${env.JOB_NAME} ${env.BUILD_NUMBER}"
        }
    }
}
