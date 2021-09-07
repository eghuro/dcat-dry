pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
		    steps{
		        script {
		            scannerHome = tool 'SonarQube';
		        }
		        withSonarQubeEnv('SonarQubeScanner') {
		            sh "${scannerHome}/bin/sonar-scanner"
		        }
		    }
		}
	}
}