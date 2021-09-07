pipeline {
	agent { label 'use' }
	stages {
		stage('Build') {
			steps {
				git credentialsId: 'fd96f917-d9f2-404d-8797-2078859754ef', url: 'ssh://git@code.eghuro.com:222/alex/dcat-dry.git'
				def customImage = docker.build("eghuro/dcat-dry")
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
	
}
