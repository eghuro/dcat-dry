pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			def scannerHome = tool 'SonarQubeScanner';
			withSonarQubeEnv('sonar') {
				sh "${scannerHome}/bin/sonar-scanner"
    		}
  		}
	}
}