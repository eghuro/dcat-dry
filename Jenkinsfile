pipeline {
	agent none
	stages {
		stage('Checkout on QA node') {
			agent { label 'use' }
			steps {
				checkout scm
				withPythonEnv('python3') {
				    sh 'python3 -m pip install --upgrade pip'
					sh 'pip install --use-feature=fast-deps --use-deprecated=legacy-resolver -r requirements.txt'
					sh 'pip check'
				}
			}
		}

		stage('Parallel QA and Docker build') {
			agent none
			parallel {
				stage('QA') {
					agent { label 'use' }
					stages {
					  	stage('Code quality') {
							steps {
								withPythonEnv('python3') {
							    	sh 'pip install radon'
									sh 'radon raw --json tsa/ > raw_report.json'
									sh 'radon cc --json tsa/ > cc_report.json'
									sh 'radon mi --json tsa/ > mi_report.json'
									sh 'flake8 tsa || true'
								}
							}
						}
						stage('SonarQube') {
							environment {
								scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';

							}
							steps {
								withSonarQubeEnv('sonar') {
									sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY"
								}
							}
						}
					}
				}
				
				stage('Docker') {
					agent { label 'app' }
					steps {
						checkout scm
			    		script {
							def customImage = docker.build("eghuro/dcat-dry")
						}
					}
				}
			}
		}
	}
}