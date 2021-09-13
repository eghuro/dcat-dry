pipeline {
	agent none
	stages {
		stage ('Dependency check') {
			agent { label 'use' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
					source /opt/conda/etc/profile.d/conda.sh
					conda create --yes -p "${WORKSPACE}@tmp/${BUILD_NUMBER}" python=3.8.8
                	conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
					pip install --use-deprecated=legacy-resolver -r requirements.txt
					pip check
					pip list --outdated
					conda deactivate
					'''
				}
			}
		}

		stage('Tests') {
			agent { label 'use' }
			steps {
				script {
					script {
					sh '''#!/usr/bin/env bash
						source /opt/conda/etc/profile.d/conda.sh
						conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
						pip install pytest-cov WebTest
						pytest --verbose --junitxml=pytest.xml --cov-report xml:cov.xml --cov=tsa
						conda deactivate
					'''
					}
				}
			}
		}

		stage('Lint') {
			agent { label 'use' }
			when {
				anyOf  {
					branch 'develop'
					branch pattern: "hotfix/.+", comparator: "REGEXP"
				}
			}
			steps {
				script {
					sh '''#!/usr/bin/env bash
						source /opt/conda/etc/profile.d/conda.sh
						conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
						pip install prospector[with_everything] types-requests types-redis
						prospector -0
						conda deactivate
					'''
				}
			}
		}

		stage('Sonar') {
			agent { label 'use' }
			when { branch 'master' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
						source /opt/conda/etc/profile.d/conda.sh
						conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
						pip install prospector[with_everything] types-requests types-redis
						prospector -0 -o pylint:prospector.txt tsa
						conda deactivate
					'''
					def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
					withSonarQubeEnv('sonar') {
						GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
						sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY -Dsonar.projectVersion=${GIT_COMMIT_HASH} -Dsonar.python.pylint.reportPaths=prospector.txt -Dsonar.junit.reportsPath=pytest.xml -Dsonar.python.coverage.reportPaths=cov.xml -Dsonar.coverage.dtdVerification=false -Dsonar.coverage.exclusions=**/__init__.py"
					}
				}
			}
		}

		stage('Cleanup') {
			agent { label 'use' }
			steps {
				script {
					'''#!/usr/bin/env bash
						source /opt/conda/etc/profile.d/conda.sh
						conda env remove -p "${WORKSPACE}@tmp/${BUILD_NUMBER}" --all
						rm -rf "${WORKSPACE}@tmp/${BUILD_NUMBER}"
					'''
				}
			}
		}

		stage('Build docker') {
			agent { label 'docker' }
			when {
				anyOf {
					branch 'develop'
					branch 'master'
					branch pattern: "hotfix/.+", comparator: "REGEXP"
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
