from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist
from django.db.models.signals import post_save
import xlsxwriter
import pandas as pd

class ExcelService:
    
    def add_meta_sheet(self, wb, meta_data):
        meta_sheet = wb.add_worksheet("Meta")
        meta_sheet.hide()
        for row_index, (key, value) in enumerate(meta_data.items()):
            meta_sheet.write(row_index, 0, key)
            meta_sheet.write(row_index, 1, value)
        
    
    def download_formatted_file(self, filename, fields, sheets_data=None, meta_data=None):
        """
        :param model: Model class (used for naming the main sheet)
        :param fields: Dictionary of field names and their types with optional choices.
        :param sheets_data: Optional dictionary to create multiple sheets.
            Format:
            {
                "Sheet1": {
                    "fields": {
                        "name": {"type": "text"},
                        "status": {"type": "list", "choices": ["Active", "Inactive"]}
                    }
                },
                "Sheet2": {
                    "fields": {
                        "project_name": {"type": "text"},
                        "priority": {"type": "list", "choices": ["High", "Medium", "Low"]}
                    }
                }
            }
        """

        wb = xlsxwriter.Workbook(filename)
        bold_format = wb.add_format({"bold": True})

        def add_sheet(wb, sheet_name, fields, start_choice_col=0):
            sheet = wb.add_worksheet(sheet_name)
            hidden_sheet = wb.add_worksheet(f"{sheet_name}_Choices")
            hidden_sheet.hide()

            current_choice_col = start_choice_col

            for col_index, (field_name, field_info) in enumerate(fields.items()):
                header = field_name
                sheet.write(0, col_index, header, bold_format)

                if field_info.get("type") == "list" and "choices" in field_info:
                    choices = field_info["choices"]
                    if choices:
                        choice_col_letter = xlsxwriter.utility.xl_col_to_name(current_choice_col)

                        # Write choices in hidden sheet
                        hidden_sheet.write(0, current_choice_col, header)
                        for row_index, choice in enumerate(choices, start=1):
                            hidden_sheet.write(row_index, current_choice_col, choice)

                        # Apply data validation dropdown
                        sheet.data_validation(
                            1, col_index, 999, col_index,
                            {
                                "validate": "list",
                                "source": f"='{sheet_name}_Choices'!${choice_col_letter}$2:${choice_col_letter}${len(choices) + 1}",
                            }
                        )
                        current_choice_col += 1
                    else:
                        sheet.data_validation(
                            1, col_index, 999, col_index,
                            {
                                "validate": "list",
                                "source": [],
                            }
                        )

        # Single sheet mode (existing behavior)
        if not sheets_data:
            add_sheet(wb, "sheet1", fields)
        else:
            # Multiple sheets mode
            for sheet_name, sheet_info in sheets_data.items():
                sheet_fields = sheet_info.get("fields", {})
                add_sheet(wb, sheet_name, sheet_fields)

        if meta_data:
            self.add_meta_sheet(wb, meta_data)
            
        wb.close()
        return filename

    def check_validations(self, file, fields):
        df = pd.read_excel(file)
        if df.empty:
            return "Excel file is empty. Please make sure the file contains data."

        df.columns = df.columns.str.lower().str.replace(" ", "_")
        df.dropna(how="all", inplace=True)

        missing_fields = [field for field in fields.keys() if field not in df.columns]
        if missing_fields:
            return f"Missing fields in Excel file: {missing_fields}. Please use the downloaded template."

        # Check for required fields with blanks
        required_fields = [field for field, info in fields.items() if info.get("required")]
        blank_required = [field for field in required_fields if df[field].isnull().any()]
        if blank_required:
            return f"Required fields cannot be blank: {blank_required}."
        return df


    @transaction.atomic
    def upload_data(self, model, file, fields, trigger_post_save=False, extra_field=None):
        df = self.check_validations(file, fields)
        if isinstance(df, str):
            return df

        data_to_create = []

        try:
            for _, row in df.iterrows():
                data = {}
                for field, info in fields.items():
                    value = row[field]
                    if pd.isna(value):
                        data[field] = None
                    else:
                        if info.get("type") == "text":
                            data[field] = value
                        elif info.get("type") == "list":
                            if info.get("related"):
                                related_model = info.get("related_model")
                                lookup_field = info.get("related_lookup", "name")  # Default lookup field is 'name'

                                if not related_model:
                                    raise ValueError(f"'related_model' not defined for field '{field}'.")

                                # Handle cases like 'code|name' in value
                                lookup_fields = [lf.strip() for lf in lookup_field.split("|")]
                                lookup_query = {lf: val.strip() for lf, val in zip(lookup_fields, value.split("|")) if val}

                                if not lookup_query:
                                    raise ValueError(f"No valid lookup values provided for related field '{field}'.")

                                related_instance = related_model.objects.filter(**lookup_query).first()

                                if not related_instance and info.get("required"):
                                    raise ObjectDoesNotExist(
                                        f"Related object not found for field '{field}' with lookup: {lookup_query}."
                                    )

                                data[field] = related_instance
                            else:
                                if value in ["True", "TRUE", True, "true"]:
                                    data[field] = True
                                elif value in ["False", "FALSE", False, "false"]:
                                    data[field] = False
                                else: 
                                    data[field] = value
                        else:
                            data[field] = value
                                
                data_to_create.append(data)

            if extra_field:
                data_to_create = [model(**{**data, **extra_field}) for data in data_to_create]
            else:
                data_to_create = [model(**data) for data in data_to_create]
            created_objects = model.objects.bulk_create(data_to_create)

            # Trigger post_save for each created object if required
            if trigger_post_save:
                for obj in created_objects:
                    post_save.send(sender=model, instance=obj, created=True)

            return True
                        
        except Exception as e:
            transaction.set_rollback(True)
            return f"Error during data upload: {str(e)}"
                        