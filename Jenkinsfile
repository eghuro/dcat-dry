pipeline {
	agent none
	stages {
		stage ('Dependency check') {
			agent { label 'use' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
					source /opt/conda/etc/profile.d/conda.sh
					conda create --yes -n ${BUILD_TAG} python=3.8.8
                	conda activate ${BUILD_TAG}
					pip install --use-deprecated=legacy-resolver -r requirements.txt
					pip check
					pip list --outdated
					'''
				}
			}
		}

		stage ('Sonar') {
			agent { label 'use' }
			when { branch 'master' }
			steps {
				script {
					pip install prospector[with_everything] types-requests
					prospector -0 --uses celery --uses flask -s veryhigh --max-line-length 200 -m -w frosted -w pyflakes -w pylint -w pyroma -W pep257 -o pylint:prospector.txt -i autoapp.py -i tsa/settings.py -i tsa/celeryconfig.py -i tsa/cache.py
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
			when {
				anyOf {
					branch 'develop'
					branch 'master'
					branch pattern: "release/.+", comparator: "REGEXP"
					buildingTag()
				}
			}
			steps {
				script {
					GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
					sh "sed -i \"s/PLACEHOLDER/${GIT_COMMIT_HASH}/g\" tsa/__init__.py"
					dockerImage = docker.build "eghuro/dcat-dry"
				}
			}
		}
		
		stage ('Push docker') {
			agent { label 'docker' }
			when {
				allOf {
					branch 'master'
					expression {
						currentBuild.result == null || currentBuild.result == 'SUCCESS'
					}
				}
			}
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
            mattermostSend "Completed ${env.JOB_NAME} ${env.BUILD_NUMBER}: ${currentBuild.currentResult}"
        }
    }
}
