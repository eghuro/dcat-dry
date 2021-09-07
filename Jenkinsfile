pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			tools {
				sonarQube 'SonarQubeScanner'
			}
			withSonarQubeEnv('sonar') {
				steps {
					sh "${scannerHome}/bin/sonar-scanner"
	    		}
    		}
  		}
	}
}