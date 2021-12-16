const conditions = "conditions";
const groups = "groups";
const entries = "entries";

db.createCollection(conditions, {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "desc"],
      properties: {
        name: {
          bsonType: "string",
          description: "Required string type",
        },
        desc: {
          bsonType: "string",
          description: "Required string type",
        },
      },
    },
  },
});
db.getCollection(conditions).createIndex({ name: 1 }, { unique: true });

db.createCollection(groups, {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "enabled"],
      properties: {
        name: {
          bsonType: "string",
          description: "Group name",
        },
        enabled: {
          bsonType: "bool",
          description: "Is group enabled",
        },
      },
    },
  },
});
db.getCollection(groups).createIndex({ name: 1 }, { unique: true });

db.createCollection(entries, {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "alarm_values",
        "condition",
        "email_timeout",
        "emails",
        "group",
        "group_id",
        "pvname",
        "subject",
        "unit",
        "warning_message",
      ],
      properties: {
        alarm_values: { bsonType: "string" },
        condition: { bsonType: "string" },
        email_timeout: { bsonType: "int" },
        emails: { bsonType: "string" },
        group: { bsonType: "string" },
        group_id: { bsonType: "objectId" },
        pvname: { bsonType: "string" },
        subject: { bsonType: "string" },
        unit: { bsonType: "string" },
        warning_message: { bsonType: "string" },
      },
    },
  },
});
