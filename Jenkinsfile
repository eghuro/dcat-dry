pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			tools {
				sonarQube 'SonarQube Scanner 2.0'
			}
			steps { 
				withSonarQubeEnv('sonar') {
					sh "${scannerHome}/bin/sonar-scanner"
	    		}
    		}
  		}
	}
}