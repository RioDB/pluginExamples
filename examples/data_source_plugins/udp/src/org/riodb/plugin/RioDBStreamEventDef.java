/*
				RioDBPlugin

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 
*/

package org.riodb.plugin;

public class RioDBStreamEventDef {

	private RioDBStreamEventField fields[];
	private int timestampNumericFieldId;

	public RioDBStreamEventDef() {
		fields = new RioDBStreamEventField[0];
		setTimestampNumericFieldId(-1);
	}

	public void addField(RioDBStreamEventField newField) {

		RioDBStreamEventField newFields[] = new RioDBStreamEventField[fields.length + 1];
		for (int i = 0; i < fields.length; i++) {
			newFields[i] = fields[i];
		}
		newFields[fields.length] = newField;
		fields = newFields;
	}

	public boolean containsField(String fieldName) {

		if (fieldName == null || fieldName.length() == 0)
			return false;
		for (RioDBStreamEventField f : fields) {
			if (fieldName.equals(f.getName())) {
				return true;
			}
		}
		return false;
	}

	public boolean[] getAllNumericFlags() {
		boolean b[] = new boolean[fields.length];
		for (int i = 0; i < fields.length; i++) {
			b[i] = fields[i].isNumeric();
		}
		return b;
	}

	public int getFieldId(String fieldName) {
		int id = -1;
		if (fieldName == null || fieldName.length() == 0)
			return id;
		for (int i = 0; i < fields.length; i++) {
			if (fieldName.toLowerCase().equals(fields[i].getName().toLowerCase())) {
				return i;
			}
		}
		return id;
	}

	public int getFieldIdCaseSensitive(String fieldName) {
		int id = -1;
		if (fieldName == null || fieldName.length() == 0)
			return id;
		for (int i = 0; i < fields.length; i++) {
			if (fieldName.equals(fields[i].getName())) {
				return i;
			}
		}
		return id;
	}

	public String getFieldList() {
		String s = "";
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].isNumeric())
				s = s + "\n{\"order\":" + String.valueOf(i) + ",\"name\":\"" + fields[i].getName()
						+ "\",\"type\":\"NUMBER\"},";
			else
				s = s + "\n{\"order\":" + String.valueOf(i) + ",\"name\":\"" + fields[i].getName()
						+ "\",\"type\":\"STRING\"},";
		}
		if (s.length() > 1)
			s = s.substring(0, s.length() - 1);
		return s;
	}

	public String getFieldName(int index) {
		return fields[index].getName();
	}

	public int getNumericFieldCount() {

		int count = 0;

		for (int i = 0; i < fields.length; i++) {
			if (fields[i].isNumeric())
				count++;
		}
		return count;
	}

	// Get the placement of a numeric field along other numeric fields in a stream.
	// for example: NAME, AGE, BIRTH_PLACE, SALARY, BID
	// AGE, SALARY and BID numeric.
	// Counting from zero, BID is the field 4
	// But among numeric fields only, BID is the field 2.
	// This means that in an Event (float[], string[]) BID field 4 is found in
	// float[2])
	public int getNumericFieldIndex(int fieldId) {
		int count = 0;
		for (int i = fieldId; i >= 0; i--) {
			if (fields[i].isNumeric())
				count++;
		}
		return count - 1;
	}

	public String getNumericFieldName(int numericFieldIndex) {
		int count = 0;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].isNumeric()) {
				if (count == numericFieldIndex) {
					return fields[i].getName();
				}
				count++;
			}

		}
		return null;
	}

	public RioDBStreamEventField getStreamField(int index) {
		return fields[index];
	}

	public RioDBStreamEventField[] getStreamFields() {
		return fields;
	}

	public int getStringFieldCount() {

		int count = 0;

		for (int i = 0; i < fields.length; i++) {
			if (!fields[i].isNumeric())
				count++;
		}
		return count;
	}

	public int getStringFieldIndex(int fieldId) {

		int count = 0;
		for (int i = fieldId; i >= 0; i--) {
			if (!fields[i].isNumeric())
				count++;
		}
		return count - 1;
	}

	public String getStringFieldName(int stringFieldIndex) {
		int count = 0;
		for (int i = 0; i < fields.length; i++) {
			if (!fields[i].isNumeric()) {
				if (count == stringFieldIndex) {
					return fields[i].getName();
				}
				count++;
			}

		}
		return null;
	}

	public String getTimestampFieldName() {
		if (timestampNumericFieldId >= 0)
			return getNumericFieldName(timestampNumericFieldId);
		return null;
	}

	public int getTimestampNumericFieldId() {
		return timestampNumericFieldId;
	}

	public boolean isNumeric(int fieldId) {
		return fields[fieldId].isNumeric();
	}

	public void setTimestampNumericFieldId(int timestampNumericFieldId) {
		this.timestampNumericFieldId = timestampNumericFieldId;
	}

	public int size() {
		return fields.length;
	}
}
