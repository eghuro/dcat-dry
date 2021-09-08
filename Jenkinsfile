pipeline {
	agent none
	stages {
		stage ('QA') {
			agent { label 'use' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
					source /opt/conda/etc/profile.d/conda.sh
					conda create --yes -n ${BUILD_TAG} python=3.8.8
                	conda activate ${BUILD_TAG}
					pip install --use-deprecated=legacy-resolver -r requirements.txt
					pip check
					pip install prospector[with_everything]
					prospector -0 --strictness high --max-line-length 200 -m -w bandit -w frosted -w mypy -w pyflakes -w pylint -w pyroma -w vulture -o pylint:prospector.txt
					'''
					def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
					withSonarQubeEnv('sonar') {
						GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
						sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY -Dsonar.projectVersion=${GIT_COMMIT_HASH} -Dsonar.python.pylint.reportPaths=prospector.txt"
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
