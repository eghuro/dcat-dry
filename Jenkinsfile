pipeline {
	agent none
	stages {
		stage ('Dependency check') {
			agent { label 'use' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
					source /opt/conda/etc/profile.d/conda.sh
					conda create --yes -p "${WORKSPACE}@tmp/${BUILD_NUMBER}" python=3.10.4
                	conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
					pip install --upgrade pip poetry
					poetry install --with test --sync
					poetry show -o
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
						prospector -0
						[[ $(radon cc -x B --total-average tsa app.py|tail -n1|cut -d' ' -f3) == "A" ]]
						conda deactivate
					'''
				}
			}
		}

		stage('Sonar') {
			agent { label 'use' }
			when { branch 'mastr' }
			steps {
				script {
					sh '''#!/usr/bin/env bash
						source /opt/conda/etc/profile.d/conda.sh
						conda activate "${WORKSPACE}@tmp/${BUILD_NUMBER}"
						prospector -0 -o pylint:prospector.txt tsa
						flake8 --radon-max-cc 10 tsa/ app.py > flake8.txt
						ochrona --report_type XML  > ochrona.xml
						conda deactivate
					'''
					def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
					withSonarQubeEnv('sonar') {
						GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
						sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY -Dsonar.projectVersion=${GIT_COMMIT_HASH} -Dsonar.python.pylint.reportPaths=prospector.txt -Dsonar.python.flake8.reportPaths=flake8.txt -Dsonar.junit.reportsPath=pytest.xml  -Dsonar.junit.reportsPath=ochrona.xml -Dsonar.python.coverage.reportPaths=cov.xml -Dsonar.coverage.dtdVerification=false -Dsonar.coverage.exclusions=**/__init__.py -Dsonar.exclusions=tsa/public/test.py"
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
					sh 'cd /home/alex/NKOD-TS; docker-compose down; docker-compose pull; docker-compose up -d --remove-orphans'
					sh 'sleep 20'
					sh 'docker exec nkod-ts_web_1 echo hello from docker'
					sh 'docker exec nkod-ts_celery_1 pip freeze'
					final String url_version = "http://app.dry.dev.eghuro.com:8088/api/v1/version"
					final def (String response, int code) = sh(script: "curl -s $url_version", returnStdout: true).trim().tokenize('\n')
					if (code == 200) {
						GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
						def json = new groovy.json.JsonSlurperClassic().parseText(response)
						assert json.revision == GIT_COMMIT_HASH
					}

					final String url_test1 = "http://app.dry.dev.eghuro.com:8088/api/v1/test/base"
					final def (String response_1, int code_1) = sh(script: "curl -s $url_test1", returnStdout: true).trim().tokenize('\n')
					println(code_1)

					final String url_test2 = "http://app.dry.dev.eghuro.com:8088/api/v1/test/job"
					final def (String response_2, int code_2) = sh(script: "curl -s $url_test2", returnStdout: true).trim().tokenize('\n')
					println(code_2)

					final String url_test3 = "http://app.dry.dev.eghuro.com:8088/api/v1/test/system"
					final def (String response_3, int code_3) = sh(script: "curl -s $url_test3", returnStdout: true).trim().tokenize('\n')
					println(code_3)
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
