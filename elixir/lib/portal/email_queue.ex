defmodule Portal.EmailQueue do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]

  schema "email_queue" do
    belongs_to :account, Portal.Account, primary_key: true
    field :id, :binary_id, primary_key: true, autogenerate: true

    field :priority, Ecto.Enum, values: [:now, :later]
    field :status, Ecto.Enum, values: [:pending, :accepted, :failed, :errored], default: :pending
    field :request, :map, redact: true
    field :response, :map, redact: true
    field :last_attempted_at, :utc_datetime_usec

    timestamps()
  end

  def changeset(%Ecto.Changeset{} = changeset) do
    changeset
    |> assoc_constraint(:account)
  end
end
