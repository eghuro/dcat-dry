pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			def scannerHome = tool 'SonarScanner 4.0';
			steps { 
				withSonarQubeEnv('sonar') {
					sh "${scannerHome}/bin/sonar-scanner"
	    		}
    		}
  		}
	}
}