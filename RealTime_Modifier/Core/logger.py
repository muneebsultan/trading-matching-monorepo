import os
import sys
import inspect
import traceback
from datetime import date, datetime, timedelta, timezone
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class Logger:
    def __init__(self, log_type):
        self.log_type = log_type
        try:
            self.log_folder = self.createLogFolder()
            print(f"✅ Log folder created at: {self.log_folder}")
        except Exception as e:
            print(f"❌ Failed to create log folder: {str(e)}")
            # Fallback to /tmp if we can't create the logs directory
            self.log_folder = f"/tmp/logs/{date.today().strftime('%d-%m-%Y')}/{self.log_type}"
            os.makedirs(self.log_folder, exist_ok=True)
            print(f"✅ Using fallback log folder: {self.log_folder}")

    def checkAndUpdateLogFolder(self):
        """Check if current date folder exists, create if needed"""
        current_date = date.today().strftime("%d-%m-%Y")
        base_log_dir = "logs"
        date_dir = os.path.join(base_log_dir, current_date)
        log_folder = os.path.join(date_dir, self.log_type)
        
        if not os.path.exists(log_folder):
            os.makedirs(log_folder, exist_ok=True)
            self.log_folder = log_folder
            print(f"✅ Created new date log directory: {log_folder}")
        return log_folder

    def log(self, exception):
        try:
            # Check and update log folder for current date
            self.checkAndUpdateLogFolder()
            
            script_name = self.getCallingScriptName()
            script_path = self.getCallingScriptPath()
            log_file = self.generateLogFilename(script_name)
            timestamp = self.getCurrentTimestamp()
            function_name = self.getCurrentFunctionName()
            line_number = self.getCurrentLineNumber()
            exception_text = self.getExceptionText(exception)
            script_args = self.getScriptArgs()
            
            formatted_message = (
                f"[{self.log_type.upper()}] [{script_path}] "
                f"[Script Args: {script_args}]"
                f"[{function_name}:{line_number}] "
                f"[{timestamp}] {exception_text}\n"
            )
            
            print(formatted_message)  # Also print to console
            self.writeToLog(formatted_message, log_file)
            
            if f'{self.log_type.upper()}' == 'ERROR':
                self.sendEmailNotification(file_name=script_path, message=self.formatEmailMessage(
                    script_path, script_args, function_name, line_number, timestamp, exception_text
                ))
        except Exception as e:
            print(f"❌ Logger error: {str(e)}")
            # Fallback to console logging if file logging fails
            print(f"[{self.log_type.upper()}] {str(exception)}")

    def formatEmailMessage(self, script_path, script_args, function_name, line_number, timestamp, exception_text):
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    padding: 20px;
                }}
                .error {{
                    color: #ff0000;
                    font-weight: bold;
                }}
                .section {{
                    margin-bottom: 10px;
                }}
            </style>
        </head>
        <body>
            <div class="error">[{self.log_type.upper()}]</div>
            <div class="section">[File Name: {script_path}]</div>
            <div class="section">[File Args: {script_args}]</div>
            <div class="section">[Function Name: {function_name}:{line_number}]</div>
            <div class="section">[Time: {timestamp}]</div>
            <div class="section">
                [Exception: {exception_text}
                <pre>
        {exception_text}
                </pre>
                ]
            </div>
        </body>
        </html>
        """

    def writeToLog(self, message, log_file):
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            with open(log_file, 'a', encoding='utf-8') as file:
                file.write(message)
                file.flush()  # Ensure the message is written immediately
            print(f"✅ Wrote log to: {log_file}")
        except Exception as e:
            print(f"❌ Failed to write to log file {log_file}: {str(e)}")
            # Fallback to console logging
            print(message)

    def createLogFolder(self):
        try:
            current_date = date.today().strftime("%d-%m-%Y")
            # Create base logs directory if it doesn't exist
            base_log_dir = "logs"
            os.makedirs(base_log_dir, exist_ok=True)
            
            # Create date directory
            date_dir = os.path.join(base_log_dir, current_date)
            os.makedirs(date_dir, exist_ok=True)
            
            # Create type directory
            log_folder = os.path.join(date_dir, self.log_type)
            os.makedirs(log_folder, exist_ok=True)
            
            print(f"✅ Created log directory: {log_folder}")
            return log_folder
        except Exception as e:
            print(f"❌ Error creating log folder: {str(e)}")
            # Fallback to /tmp if we can't create the logs directory
            fallback_dir = f"/tmp/logs/{date.today().strftime('%d-%m-%Y')}/{self.log_type}"
            os.makedirs(fallback_dir, exist_ok=True)
            print(f"✅ Using fallback log directory: {fallback_dir}")
            return fallback_dir

    def generateLogFilename(self, script_name):
        log_filename = f"{script_name}.log"
        log_file = os.path.join(self.log_folder, log_filename)
        return log_file

    def getCallingScriptName(self):
        stack = inspect.stack()
        calling_frame = stack[2]
        module = inspect.getmodule(calling_frame[0])
        # script_name = os.path.basename(module.__file__)
        # return script_name
        if module and hasattr(module, '__file__'):
            return os.path.basename(module.__file__)
        else:
            return "unknown_script"

    
    def getCallingScriptPath(self):
        stack = inspect.stack()
        calling_frame = stack[2]
        module = inspect.getmodule(calling_frame[0])
        script_path = os.path.abspath(module.__file__)
        # return script_path
        if module and hasattr(module, '__file__'):
            return os.path.abspath(module.__file__)
        else:
            return "unknown_path"

    def getCurrentTimestamp(self):
        utc_now = datetime.now(tz=timezone(timedelta(hours=5)))
        timestamp = utc_now.strftime("%H:%M:%S")
        return timestamp

    def getCurrentFunctionName(self):
        stack = inspect.stack()
        function_name = stack[2].function
        return function_name

    def getCurrentLineNumber(self):
        stack = inspect.stack()
        line_number = stack[2].lineno
        return line_number
    
    def getExceptionText(self, exception):
        if isinstance(exception, Exception):
            exception_type = type(exception).__name__
            exception_traceback = traceback.format_exc()
            exception_text = f"{exception_type}: {exception}\n{exception_traceback}"
        else:
            exception_text = str(exception)
        return exception_text
    
    def getScriptArgs(self):
        script_args = sys.argv[1:]
        return f"{' '.join(script_args)}"
    
    def sendEmailNotification(self ,file_name ,message):
        try:
            email_list=['ahsan.officefield@gmail.com','saad.officefield@gmail.com']
            content = Mail(from_email='python_logs@traderverse.io',
                            to_emails=email_list,
                            subject=f"{self.log_type.upper()}: {file_name}",
                            html_content= message )
        
            sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
            sg.send(content)
        except Exception as e :
            print(e)
