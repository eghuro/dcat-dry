pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			tools {
				sonarQube 'SonarQubeScanner'
			}
			steps { 
				withSonarQubeEnv('sonar') {
					sh "${scannerHome}/bin/sonar-scanner"
	    		}
    		}
  		}
	}
}