pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			steps {
				def scannerHome = tool 'SonarQubeScanner';
				withSonarQubeEnv('sonar') {
					sh "${scannerHome}/bin/sonar-scanner"
	    		}
    		}
  		}
	}
}