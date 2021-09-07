pipeline {
	agent any
	stages {
		stage('SonarQube analysis') {
			environment {
			    SCANNER_HOME = tool 'SonarQubeScanner'
    			PROJECT_NAME = "DCAT-DRY"
		  	}
			tools {
				sonarQube 'SonarQube Scanner'
			}
			steps { 
				withSonarQubeEnv('SonarQubeScanner') {
			        sh '''$SCANNER_HOME/bin/sonar-scanner 
			        -Dsonar.java.binaries=build/classes/java/ \
			        -Dsonar.projectKey=$PROJECT_NAME \
			        -Dsonar.sources=.'''
			    }
    		}
  		}
	}
}