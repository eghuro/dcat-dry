pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
		    steps{
		        withSonarQubeEnv('SonarQubeScanner', envOnly: true) {
		            println ${env.SONAR_HOST_URL}
		        }
		    }
		}
	}
}