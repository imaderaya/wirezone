defmodule Portal.Mailer do
  alias Swoosh.Mailer
  alias Swoosh.Email
  alias Portal.Mailer.RateLimiter
  require Logger

  def deliver_with_rate_limit(email, config \\ []) do
    {key, config} = Keyword.pop(config, :rate_limit_key, {email.to, email.subject})
    {rate_limit, config} = Keyword.pop(config, :rate_limit, 10)
    {rate_limit_interval, config} = Keyword.pop(config, :rate_limit_interval, :timer.minutes(2))

    RateLimiter.rate_limit(key, rate_limit, rate_limit_interval, fn ->
      deliver(email, config)
    end)
    |> case do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Delivers an email via configured Swoosh adapter.

  If adapter is not configured or is set to nil, the delivery will be ignored and
  function will return `{:ok, %{}}`.

  Notice: this code is copied from `Swoosh.Mailer.deliver/2` and modified to
  not send emails if adapter is not configured. This is needed to avoid
  custom adapter implementation that does nothing.
  """
  def deliver(email, config \\ []) do
    opts = Mailer.parse_config(:portal, __MODULE__, [], config)
    metadata = %{email: email, config: config, mailer: __MODULE__}

    if opts[:adapter] do
      :telemetry.span([:swoosh, :deliver], metadata, fn ->
        case Mailer.deliver(email, opts) do
          {:ok, result} -> {{:ok, result}, Map.put(metadata, :result, result)}
          {:error, error} -> {{:error, error}, Map.put(metadata, :error, error)}
        end
      end)
    else
      Logger.info("Emails are not configured", email_subject: inspect(email.subject))
      {:ok, %{}}
    end
  end

  # sobelow_skip ["DOS.StringToAtom"]
  defp render_template(view, template, format, assigns) do
    heex = apply(view, String.to_atom("#{template}_#{format}"), [assigns])
    assigns = Keyword.merge(assigns, inner_content: heex)
    Phoenix.Template.render_to_string(view, "#{template}_#{format}", "html", assigns)
  end

  def render_body(%Swoosh.Email{} = email, view, template, assigns) do
    assigns = assigns ++ [email: email]

    email
    |> Email.html_body(render_template(view, template, "html", assigns))
    |> Email.text_body(render_template(view, template, "text", assigns))
  end

  def render_text_body(%Swoosh.Email{} = email, view, template, assigns) do
    assigns = assigns ++ [email: email]

    email
    |> Email.text_body(render_template(view, template, "text", assigns))
  end

  def active? do
    mailer_config = Portal.Config.fetch_env!(:portal, Portal.Mailer)
    mailer_config[:from_email] && mailer_config[:adapter]
  end

  def default_email do
    # Fail hard if email not configured
    from_email =
      Portal.Config.fetch_env!(:portal, Portal.Mailer)
      |> Keyword.fetch!(:from_email)

    Email.new()
    |> Email.from({"Firezone Notifications", from_email})
  end

  def with_account(%Email{} = email, account_id) do
    Email.put_private(email, :account_id, account_id)
  end

  @doc """
  Enqueues an email for delivery via the email queue worker.

  priority: :now — delivers inline and tracks result in the DB row.
  priority: :later — inserts only; worker delivers asynchronously.
  """
  def enqueue(%Email{} = email, :now) do
    account_id = email.private[:account_id]
    request = serialize(email)

    with {:ok, entry} <-
           %Portal.EmailQueue{
             account_id: account_id,
             priority: :now,
             request: request,
             last_attempted_at: DateTime.utc_now()
           }
           |> Portal.Safe.unscoped()
           |> Portal.Safe.insert() do
      email_to_deliver = Map.put(email, :provider_options, %{operation_id: entry.id})

      {status, response} =
        case deliver(email_to_deliver) do
          {:ok, resp} ->
            {:accepted, resp}

          {:error, {code, body}} when is_integer(code) and code in 400..499 ->
            {:failed, %{"status" => code, "body" => inspect(body)}}

          {:error, reason} ->
            {:errored, %{"reason" => inspect(reason)}}
        end

      entry
      |> Ecto.Changeset.change(status: status, response: response)
      |> Portal.Safe.unscoped()
      |> Portal.Safe.update()

      {:ok, entry}
    end
  end

  def enqueue(%Email{} = email, :later) do
    account_id = email.private[:account_id]

    %Portal.EmailQueue{account_id: account_id, priority: :later, request: serialize(email)}
    |> Portal.Safe.unscoped()
    |> Portal.Safe.insert()
  end

  defp serialize(%Email{} = email) do
    %{
      "to" => Enum.map(email.to, fn {name, addr} -> %{"name" => name, "address" => addr} end),
      "from" =>
        case email.from do
          {name, addr} -> %{"name" => name, "address" => addr}
          addr -> %{"name" => "", "address" => addr}
        end,
      "subject" => email.subject,
      "html_body" => email.html_body,
      "text_body" => email.text_body
    }
  end
end
