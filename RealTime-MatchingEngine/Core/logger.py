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
        

    def log(self, exception):
        log_folder = self.createLogFolder()
        script_name = self.getCallingScriptName()
        script_path = self.getCallingScriptPath()
        log_file = self.generateLogFilename(script_name, log_folder)
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
        self.writeToLog(formatted_message, log_file)
        print()
        email_message =f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <style>
                                /* Define CSS styles for better formatting */
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

        if f'{self.log_type.upper()}' == 'ERROR':
            self.sendEmailNotification(file_name = script_path ,message = email_message)

    # def writeToLog(self, message, log_file):
    #     with open(log_file, 'a') as file:
    #         file.write(message)
    def writeToLog(self, message, log_file):
        with open(log_file, 'a', encoding='utf-8') as file:  # Set encoding to UTF-8
            file.write(message)

    def createLogFolder(self):
        current_date = date.today().strftime("%d-%m-%Y")
        log_folder = f"logs/{current_date}/{self.log_type}"
        os.makedirs(log_folder, exist_ok=True)
        return log_folder

    def generateLogFilename(self, script_name, log_folder):
        log_filename = f"{script_name}.log"
        log_file = os.path.join(log_folder, log_filename)
        return log_file

    def getCallingScriptName(self):
        stack = inspect.stack()
        calling_frame = stack[2]
        module = inspect.getmodule(calling_frame[0])
        script_name = os.path.basename(module.__file__)
        return script_name
    
    def getCallingScriptPath(self):
        stack = inspect.stack()
        calling_frame = stack[2]
        module = inspect.getmodule(calling_frame[0])
        script_path = os.path.abspath(module.__file__)
        return script_path

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
