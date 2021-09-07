pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			def scannerHome = tool 'SonarScanner 4.0';
			withSonarQubeEnv('sonar') {
				sh "${scannerHome}/bin/sonar-scanner"
    		}
  		}
	}
}