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
						prospector -0 -o pylint:/tmp/prospector.txt tsa
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
				dir("${env.WORKSPACE}@tmp") {
		         	   deleteDir()
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
		
		stage('Deploy') {
			agent { label 'dry-prod' }
		 	options { skipDefaultCheckout() }
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
					sh 'cd /home/alex/NKOD-TS; docker-compose down; docker-compose pull; redis-cli -h 10.114.0.2 -n 0 flushdb; redis-cli -h 10.114.0.2 -n 1 flushdb; docker-compose up -d --remove-orphans; docker image prune'
					sh 'sleep 120'
					sh 'docker exec nkod-ts_web_1 echo hello from docker'
					sh 'docker exec nkod-ts_celery_1 pip freeze'
					final String url_version = "https://app.dry.dev.eghuro.com/api/v1/version"
					final def (String response, int code) = sh(script: "curl -s $url_version", returnStdout: true).trim().tokenize('\n')
					if (code == 200) {
						GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
						def json = new groovy.json.JsonSlurperClassic().parseText(response)
						assert json.revision == GIT_COMMIT_HASH
					}

					final String url_test1 = "https://app.dry.dev.eghuro.com/api/v1/test/base"
					final def (String response, int code) = sh(script: "curl -s $url_test1", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test2 = "https://app.dry.dev.eghuro.com/api/v1/test/job"
					final def (String response, int code) = sh(script: "curl -s $url_test2", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test3 = "https://app.dry.dev.eghuro.com/api/v1/test/system"
					final def (String response, int code) = sh(script: "curl -s $url_test3", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test4 = "https://app.dry.dev.eghuro.com/api/v1/test/analyze"
					final def (String response, int code) = sh(script: "curl -s $url_test4", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test5 = "https://app.dry.dev.eghuro.com/api/v1/test/process"
					final def (String response, int code) = sh(script: "curl -s $url_test5", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test6 = "https://app.dry.dev.eghuro.com/api/v1/test/dereference/1"
					final def (String response, int code) = sh(script: "curl -s $url_test6", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test7 = "https://app.dry.dev.eghuro.com/api/v1/test/dereference/2"
					final def (String response, int code) = sh(script: "curl -s $url_test7", returnStdout: true).trim().tokenize('\n')
					assert code == 200

					final String url_test8 = "https://app.dry.dev.eghuro.com/api/v1/test/dereference/3"
					final def (String response, int code) = sh(script: "curl -s $url_test8", returnStdout: true).trim().tokenize('\n')
					assert code == 200
				}
			}
		}

		stage('Execute') {
			agent { label 'dry-prod' }
			options { skipDefaultCheckout() }
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
					sh 'docker exec nkod-ts_web_1 flask batch -g /tmp/graphs.txt -s http://10.114.0.2:8890/sparql'
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
